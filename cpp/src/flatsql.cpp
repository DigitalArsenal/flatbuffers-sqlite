#include "flatsql/database.h"
#include <iostream>
#include <fstream>
#include <vector>
#include <cstring>

#ifdef __EMSCRIPTEN__
#include <emscripten/bind.h>
#include <emscripten/val.h>

using namespace emscripten;

namespace flatsql {

// JavaScript-friendly wrapper for Value
val valueToJS(const Value& v) {
    return std::visit([](const auto& value) -> val {
        using T = std::decay_t<decltype(value)>;
        if constexpr (std::is_same_v<T, std::monostate>) {
            return val::null();
        } else if constexpr (std::is_same_v<T, bool>) {
            return val(value);
        } else if constexpr (std::is_same_v<T, std::string>) {
            return val(value);
        } else if constexpr (std::is_same_v<T, std::vector<uint8_t>>) {
            return val(typed_memory_view(value.size(), value.data()));
        } else {
            return val(static_cast<double>(value));
        }
    }, v);
}

// JavaScript-friendly wrapper for QueryResult
struct JSQueryResult {
    std::vector<std::string> columns;
    std::vector<std::vector<val>> rows;
    size_t rowCount;

    JSQueryResult(const QueryResult& result) : columns(result.columns), rowCount(result.rowCount()) {
        for (const auto& row : result.rows) {
            std::vector<val> jsRow;
            for (const auto& cell : row) {
                jsRow.push_back(valueToJS(cell));
            }
            rows.push_back(std::move(jsRow));
        }
    }

    val getColumns() const {
        val arr = val::array();
        for (const auto& col : columns) {
            arr.call<void>("push", val(col));
        }
        return arr;
    }

    val getRows() const {
        val arr = val::array();
        for (const auto& row : rows) {
            val jsRow = val::array();
            for (const auto& cell : row) {
                jsRow.call<void>("push", cell);
            }
            arr.call<void>("push", jsRow);
        }
        return arr;
    }

    size_t getRowCount() const { return rowCount; }
};

// JavaScript-friendly wrapper for FlatSQLDatabase
class JSFlatSQLDatabase {
public:
    JSFlatSQLDatabase(const std::string& schemaSource, const std::string& dbName = "default")
        : db_(FlatSQLDatabase::fromSchema(schemaSource, dbName)) {}

    // Register file identifier for routing
    void registerFileId(const std::string& fileId, const std::string& tableName) {
        db_.registerFileId(fileId, tableName);
    }

    // Ingest size-prefixed FlatBuffers from Uint8Array
    double ingest(const std::vector<uint8_t>& data) {
        return static_cast<double>(db_.ingest(data.data(), data.size()));
    }

    // Ingest a single FlatBuffer (without size prefix)
    double ingestOne(const std::vector<uint8_t>& data) {
        return static_cast<double>(db_.ingestOne(data.data(), data.size()));
    }

    // Load and rebuild from exported data
    void loadAndRebuild(const std::vector<uint8_t>& data) {
        db_.loadAndRebuild(data.data(), data.size());
    }

    JSQueryResult query(const std::string& sql) {
        return JSQueryResult(db_.query(sql));
    }

    val exportData() {
        std::vector<uint8_t> data = db_.exportData();
        val result = val::global("Uint8Array").new_(data.size());
        val memView = val(typed_memory_view(data.size(), data.data()));
        result.call<void>("set", memView);
        return result;
    }

    val listTables() {
        std::vector<std::string> tables = db_.listTables();
        val result = val::array();
        for (const auto& t : tables) {
            result.call<void>("push", val(t));
        }
        return result;
    }

    val getStats() {
        auto stats = db_.getStats();
        val result = val::array();
        for (const auto& s : stats) {
            val stat = val::object();
            stat.set("tableName", val(s.tableName));
            stat.set("fileId", val(s.fileId));
            stat.set("recordCount", val(static_cast<double>(s.recordCount)));
            val indexes = val::array();
            for (const auto& idx : s.indexes) {
                indexes.call<void>("push", val(idx));
            }
            stat.set("indexes", indexes);
            result.call<void>("push", stat);
        }
        return result;
    }

private:
    FlatSQLDatabase db_;
};

EMSCRIPTEN_BINDINGS(flatsql) {
    register_vector<uint8_t>("VectorUint8");

    class_<JSQueryResult>("QueryResult")
        .function("getColumns", &JSQueryResult::getColumns)
        .function("getRows", &JSQueryResult::getRows)
        .function("getRowCount", &JSQueryResult::getRowCount)
        ;

    class_<JSFlatSQLDatabase>("FlatSQLDatabase")
        .constructor<const std::string&>()
        .constructor<const std::string&, const std::string&>()
        .function("registerFileId", &JSFlatSQLDatabase::registerFileId)
        .function("ingest", &JSFlatSQLDatabase::ingest)
        .function("ingestOne", &JSFlatSQLDatabase::ingestOne)
        .function("loadAndRebuild", &JSFlatSQLDatabase::loadAndRebuild)
        .function("query", &JSFlatSQLDatabase::query)
        .function("exportData", &JSFlatSQLDatabase::exportData)
        .function("listTables", &JSFlatSQLDatabase::listTables)
        .function("getStats", &JSFlatSQLDatabase::getStats)
        ;
}

}  // namespace flatsql

#else
// Native build - CLI tool with stdin piping support

namespace flatsql {

void printUsage(const char* prog) {
    std::cerr << "Usage: " << prog << " [options]\n"
              << "\n"
              << "Streaming FlatBuffer SQL engine - pipe size-prefixed FlatBuffers to stdin\n"
              << "\n"
              << "Options:\n"
              << "  --schema <file>     Schema file (IDL format)\n"
              << "  --map <id>=<table>  Map file identifier to table (repeatable)\n"
              << "  --query <sql>       SQL query to run after ingesting\n"
              << "  --export <file>     Export storage to file after ingesting\n"
              << "  --load <file>       Load existing storage file before stdin\n"
              << "  --stats             Print statistics after ingesting\n"
              << "  --help              Show this help\n"
              << "\n"
              << "Example:\n"
              << "  cat data.fb | " << prog << " --schema app.fbs --map USER=User --query 'SELECT * FROM User'\n"
              << "\n"
              << "Stream format: [4-byte size LE][FlatBuffer][4-byte size LE][FlatBuffer]...\n"
              << "Each FlatBuffer must have file_identifier at bytes 4-7.\n";
}

int runCLI(int argc, char* argv[]) {
    std::string schemaFile;
    std::string querySQL;
    std::string exportFile;
    std::string loadFile;
    std::vector<std::pair<std::string, std::string>> fileIdMappings;
    bool showStats = false;

    // Parse arguments
    for (int i = 1; i < argc; i++) {
        std::string arg = argv[i];
        if (arg == "--help" || arg == "-h") {
            printUsage(argv[0]);
            return 0;
        } else if (arg == "--schema" && i + 1 < argc) {
            schemaFile = argv[++i];
        } else if (arg == "--map" && i + 1 < argc) {
            std::string mapping = argv[++i];
            size_t eq = mapping.find('=');
            if (eq != std::string::npos) {
                fileIdMappings.emplace_back(
                    mapping.substr(0, eq),
                    mapping.substr(eq + 1)
                );
            }
        } else if (arg == "--query" && i + 1 < argc) {
            querySQL = argv[++i];
        } else if (arg == "--export" && i + 1 < argc) {
            exportFile = argv[++i];
        } else if (arg == "--load" && i + 1 < argc) {
            loadFile = argv[++i];
        } else if (arg == "--stats") {
            showStats = true;
        }
    }

    if (schemaFile.empty()) {
        std::cerr << "Error: --schema is required\n";
        printUsage(argv[0]);
        return 1;
    }

    // Read schema file
    std::ifstream schemaStream(schemaFile);
    if (!schemaStream) {
        std::cerr << "Error: Cannot open schema file: " << schemaFile << "\n";
        return 1;
    }
    std::string schemaSource((std::istreambuf_iterator<char>(schemaStream)),
                              std::istreambuf_iterator<char>());

    // Create database
    FlatSQLDatabase db = FlatSQLDatabase::fromSchema(schemaSource, "cli_db");

    // Register file ID mappings
    for (const auto& [fileId, tableName] : fileIdMappings) {
        db.registerFileId(fileId, tableName);
    }

    // Load existing data if specified
    if (!loadFile.empty()) {
        std::ifstream loadStream(loadFile, std::ios::binary);
        if (!loadStream) {
            std::cerr << "Error: Cannot open load file: " << loadFile << "\n";
            return 1;
        }
        std::vector<uint8_t> loadData((std::istreambuf_iterator<char>(loadStream)),
                                       std::istreambuf_iterator<char>());
        db.loadAndRebuild(loadData.data(), loadData.size());
        std::cerr << "Loaded " << loadData.size() << " bytes from " << loadFile << "\n";
    }

    // Read and ingest from stdin
    std::vector<uint8_t> buffer;
    constexpr size_t CHUNK_SIZE = 64 * 1024;
    char chunk[CHUNK_SIZE];

    std::cin.rdbuf()->pubsetbuf(nullptr, 0);  // Unbuffered stdin
    while (std::cin.read(chunk, CHUNK_SIZE) || std::cin.gcount() > 0) {
        size_t bytesRead = static_cast<size_t>(std::cin.gcount());
        buffer.insert(buffer.end(), chunk, chunk + bytesRead);

        // Process complete records
        size_t ingested = db.ingest(buffer.data(), buffer.size());
        if (ingested > 0) {
            std::cerr << "Ingested " << ingested << " records\n";
        }

        // Calculate bytes consumed (need to track this properly)
        // For now, after ingest we need to remove processed data
        // The storage already handles partial buffers, so we could track offset
    }

    // Final ingest of any remaining data
    if (!buffer.empty()) {
        size_t ingested = db.ingest(buffer.data(), buffer.size());
        if (ingested > 0) {
            std::cerr << "Ingested " << ingested << " final records\n";
        }
    }

    // Show stats if requested
    if (showStats) {
        auto stats = db.getStats();
        std::cerr << "\nDatabase Statistics:\n";
        for (const auto& s : stats) {
            std::cerr << "  Table: " << s.tableName;
            if (!s.fileId.empty()) {
                std::cerr << " (file_id: " << s.fileId << ")";
            }
            std::cerr << " - " << s.recordCount << " records";
            if (!s.indexes.empty()) {
                std::cerr << ", indexes: ";
                for (size_t i = 0; i < s.indexes.size(); i++) {
                    if (i > 0) std::cerr << ", ";
                    std::cerr << s.indexes[i];
                }
            }
            std::cerr << "\n";
        }
    }

    // Execute query if specified
    if (!querySQL.empty()) {
        try {
            QueryResult result = db.query(querySQL);

            // Print columns
            for (size_t i = 0; i < result.columns.size(); i++) {
                if (i > 0) std::cout << "\t";
                std::cout << result.columns[i];
            }
            std::cout << "\n";

            // Print rows
            for (const auto& row : result.rows) {
                for (size_t i = 0; i < row.size(); i++) {
                    if (i > 0) std::cout << "\t";
                    std::visit([](const auto& v) {
                        using T = std::decay_t<decltype(v)>;
                        if constexpr (std::is_same_v<T, std::monostate>) {
                            std::cout << "NULL";
                        } else if constexpr (std::is_same_v<T, std::string>) {
                            std::cout << v;
                        } else if constexpr (std::is_same_v<T, bool>) {
                            std::cout << (v ? "true" : "false");
                        } else if constexpr (std::is_same_v<T, std::vector<uint8_t>>) {
                            std::cout << "[" << v.size() << " bytes]";
                        } else {
                            std::cout << v;
                        }
                    }, row[i]);
                }
                std::cout << "\n";
            }
        } catch (const std::exception& e) {
            std::cerr << "Query error: " << e.what() << "\n";
            return 1;
        }
    }

    // Export if specified
    if (!exportFile.empty()) {
        std::vector<uint8_t> exportData = db.exportData();
        std::ofstream out(exportFile, std::ios::binary);
        if (!out) {
            std::cerr << "Error: Cannot open export file: " << exportFile << "\n";
            return 1;
        }
        out.write(reinterpret_cast<const char*>(exportData.data()), exportData.size());
        std::cerr << "Exported " << exportData.size() << " bytes to " << exportFile << "\n";
    }

    return 0;
}

}  // namespace flatsql

int main(int argc, char* argv[]) {
    return flatsql::runCLI(argc, argv);
}

#endif
