#include "flatsql/database.h"
#include "flatsql/junction.h"
#include <iostream>
#include <cassert>

using namespace flatsql;

void testSchemaParser() {
    std::cout << "Testing schema parser..." << std::endl;

    // Test IDL parsing
    std::string idl = R"(
        table User {
            id: int (id);
            name: string;
            email: string (key);
            age: int;
        }

        table Post {
            id: int (id);
            user_id: int (key);
            title: string;
            content: string;
        }
    )";

    DatabaseSchema schema = SchemaParser::parseIDL(idl, "test_db");

    assert(schema.name == "test_db");
    assert(schema.tables.size() == 2);

    const TableDef* userTable = schema.getTable("User");
    assert(userTable != nullptr);
    assert(userTable->columns.size() == 4);
    assert(userTable->columns[0].name == "id");
    assert(userTable->columns[0].type == ValueType::Int32);
    assert(userTable->columns[0].primaryKey == true);

    std::cout << "Schema parser tests passed!" << std::endl;
}

void testSQLParser() {
    std::cout << "Testing SQL parser..." << std::endl;

    // Test SELECT
    ParsedSQL select = SQLParser::parse("SELECT name, email FROM users WHERE age > 18 LIMIT 10");
    assert(select.type == SQLStatementType::Select);
    assert(select.tableName == "users");
    assert(select.columns.size() == 2);
    assert(select.columns[0] == "name");
    assert(select.columns[1] == "email");
    assert(select.where.has_value());
    assert(select.where->column == "age");
    assert(select.where->op == ">");
    assert(select.limit.has_value());
    assert(select.limit.value() == 10);

    // Test BETWEEN
    ParsedSQL between = SQLParser::parse("SELECT * FROM orders WHERE amount BETWEEN 100 AND 500");
    assert(between.where.has_value());
    assert(between.where->hasBetween);

    // Test INSERT
    ParsedSQL insert = SQLParser::parse("INSERT INTO users (name, age) VALUES ('John', 25)");
    assert(insert.type == SQLStatementType::Insert);
    assert(insert.tableName == "users");
    assert(insert.columns.size() == 2);
    assert(insert.insertValues.size() == 2);

    std::cout << "SQL parser tests passed!" << std::endl;
}

void testBTree() {
    std::cout << "Testing B-tree..." << std::endl;

    BTree tree(ValueType::Int32, 4);  // Small order for testing

    // Insert some values
    for (int i = 0; i < 100; i++) {
        tree.insert(i, static_cast<uint64_t>(i * 100), 50, static_cast<uint64_t>(i));
    }

    assert(tree.getEntryCount() == 100);

    // Search for specific value
    auto results = tree.search(42);
    assert(results.size() == 1);
    assert(results[0].dataOffset == 4200);

    // Range search
    auto rangeResults = tree.range(10, 20);
    assert(rangeResults.size() == 11);  // 10 through 20 inclusive

    // Get all
    auto all = tree.all();
    assert(all.size() == 100);

    std::cout << "B-tree tests passed!" << std::endl;
}

void testStorage() {
    std::cout << "Testing streaming FlatBuffer storage..." << std::endl;

    StreamingFlatBufferStore store;

    // Create fake FlatBuffer data with file identifiers at bytes 4-7
    // Format: [root offset 4 bytes][file_id 4 bytes][data...]
    std::vector<uint8_t> data1 = {0x08, 0x00, 0x00, 0x00, 'U', 'S', 'E', 'R', 0x0C, 0x00};
    std::vector<uint8_t> data2 = {0x08, 0x00, 0x00, 0x00, 'P', 'O', 'S', 'T', 0x0C, 0x00, 0x04, 0x00};

    // Track ingested sequences
    std::vector<std::pair<std::string, uint64_t>> ingested;

    // ingestFlatBuffer returns sequence (stable rowid)
    uint64_t seq1 = store.ingestFlatBuffer(data1.data(), data1.size(),
        [&](std::string_view fileId, const uint8_t*, size_t, uint64_t seq, uint64_t) {
            ingested.push_back({std::string(fileId), seq});
        });
    uint64_t seq2 = store.ingestFlatBuffer(data2.data(), data2.size(),
        [&](std::string_view fileId, const uint8_t*, size_t, uint64_t seq, uint64_t) {
            ingested.push_back({std::string(fileId), seq});
        });

    assert(store.getRecordCount() == 2);
    assert(seq1 == 1);  // First sequence is 1
    assert(seq2 == 2);  // Second sequence is 2

    // Verify file identifiers were extracted
    assert(ingested.size() == 2);
    assert(ingested[0].first == "USER");
    assert(ingested[1].first == "POST");

    // Read back by sequence
    StoredRecord record1 = store.readRecord(seq1);
    assert(record1.header.fileId == "USER");
    assert(record1.data == data1);
    assert(record1.header.sequence == seq1);

    StoredRecord record2 = store.readRecord(seq2);
    assert(record2.header.fileId == "POST");
    assert(record2.data == data2);
    assert(record2.header.sequence == seq2);

    // Test hasRecord
    assert(store.hasRecord(seq1));
    assert(store.hasRecord(seq2));
    assert(!store.hasRecord(999));

    // Export and reload
    auto exportedData = store.exportData();

    StreamingFlatBufferStore reloaded;
    std::vector<std::pair<std::string, uint64_t>> reloadedIngested;
    reloaded.loadAndRebuild(exportedData.data(), exportedData.size(),
        [&](std::string_view fileId, const uint8_t*, size_t, uint64_t seq, uint64_t) {
            reloadedIngested.push_back({std::string(fileId), seq});
        });

    assert(reloaded.getRecordCount() == 2);

    // Verify records can still be read by sequence after reload
    StoredRecord reloadedRecord1 = reloaded.readRecord(1);
    assert(reloadedRecord1.header.fileId == "USER");
    assert(reloadedRecord1.data == data1);

    std::cout << "Storage tests passed!" << std::endl;
}

void testDatabase() {
    std::cout << "Testing FlatSQL database..." << std::endl;

    std::string schema = R"(
        table items {
            id: int (id);
            name: string;
            price: float;
        }
    )";

    FlatSQLDatabase db = FlatSQLDatabase::fromSchema(schema, "test");

    auto tables = db.listTables();
    assert(tables.size() == 1);
    assert(tables[0] == "items");

    const TableDef* itemsDef = db.getTableDef("items");
    assert(itemsDef != nullptr);
    assert(itemsDef->columns.size() == 3);

    // Register file identifier for routing
    db.registerFileId("ITEM", "items");

    // Create fake FlatBuffer data with file identifier "ITEM" at bytes 4-7
    std::vector<uint8_t> fakeData = {0x08, 0x00, 0x00, 0x00, 'I', 'T', 'E', 'M'};

    // Ingest using streaming API
    db.ingestOne(fakeData.data(), fakeData.size());
    db.ingestOne(fakeData.data(), fakeData.size());

    // Query
    QueryResult result = db.query("SELECT * FROM items");
    assert(result.rowCount() == 2);

    std::cout << "Database tests passed!" << std::endl;
}

void testSchemaAnalyzer() {
    std::cout << "Testing schema analyzer..." << std::endl;

    SchemaAnalyzer analyzer;

    // Add a schema with tables and references
    std::string weaponSchema = R"(
        namespace game;
        table Weapon {
            name: string;
            damage: int;
        }
    )";

    std::string monsterSchema = R"(
        include "weapons.fbs";
        namespace game;

        table Monster {
            name: string;
            hp: int;
            weapon: Weapon;
            inventory: [Weapon];
        }
    )";

    analyzer.addSchema("weapons.fbs", weaponSchema);
    analyzer.addSchema("monster.fbs", monsterSchema);

    SchemaAnalysis analysis = analyzer.analyze();

    // Verify no cycles
    assert(!analysis.cycle.has_value() || !analysis.cycle->hasCycle);
    std::cout << "  No circular dependencies detected" << std::endl;

    // Verify import order
    assert(analysis.importOrder.size() == 2);
    std::cout << "  Import order: ";
    for (const auto& file : analysis.importOrder) {
        std::cout << file << " ";
    }
    std::cout << std::endl;

    // Verify tables detected
    assert(analysis.tables.count("Weapon") > 0);
    assert(analysis.tables.count("Monster") > 0);
    std::cout << "  Found tables: Weapon, Monster" << std::endl;

    // Verify junction tables generated
    std::cout << "  Junction tables: " << analysis.junctionTables.size() << std::endl;
    for (const auto& jt : analysis.junctionTables) {
        std::cout << "    - " << jt.name << " (";
        switch (jt.relationType) {
            case RelationType::SINGLE_TABLE: std::cout << "single"; break;
            case RelationType::VECTOR_TABLE: std::cout << "vector"; break;
            case RelationType::UNION: std::cout << "union"; break;
            case RelationType::VECTOR_UNION: std::cout << "vector_union"; break;
        }
        std::cout << ")" << std::endl;
    }

    // Verify struct detection
    assert(!analyzer.isStruct("Weapon"));  // Weapon is a table
    assert(!analyzer.isStruct("Monster")); // Monster is a table

    std::cout << "Schema analyzer tests passed!" << std::endl;
}

void testCycleDetection() {
    std::cout << "Testing cycle detection..." << std::endl;

    SchemaAnalyzer analyzer;

    // Create schemas with circular dependency
    std::string schemaA = R"(
        include "b.fbs";
        table A { b: B; }
    )";

    std::string schemaB = R"(
        include "c.fbs";
        table B { c: C; }
    )";

    std::string schemaC = R"(
        include "a.fbs";
        table C { a: A; }
    )";

    analyzer.addSchema("a.fbs", schemaA);
    analyzer.addSchema("b.fbs", schemaB);
    analyzer.addSchema("c.fbs", schemaC);

    SchemaAnalysis analysis = analyzer.analyze();

    // Should detect cycle
    assert(analysis.cycle.has_value());
    assert(analysis.cycle->hasCycle);
    std::cout << "  Cycle detected: ";
    for (const auto& node : analysis.cycle->cyclePath) {
        std::cout << node << " -> ";
    }
    std::cout << std::endl;

    // Should have error message
    assert(!analysis.errors.empty());
    std::cout << "  Error: " << analysis.errors[0] << std::endl;

    std::cout << "Cycle detection tests passed!" << std::endl;
}

void testJunctionManager() {
    std::cout << "Testing junction manager..." << std::endl;

    std::string schema = R"(
        table Monster {
            id: int (id);
            name: string;
        }
        table Weapon {
            id: int (id);
            name: string;
            damage: int;
        }
    )";

    auto db = FlatSQLDatabase::fromSchema(schema, "junction_test");

    // Create analysis with junction table definition
    SchemaAnalysis analysis;
    TableInfo monsterInfo;
    monsterInfo.name = "Monster";
    monsterInfo.sourceFile = "test.fbs";
    monsterInfo.isImported = false;

    TableReference weaponRef;
    weaponRef.fieldName = "weapon";
    weaponRef.referencedType = "Weapon";
    weaponRef.relationType = RelationType::SINGLE_TABLE;
    monsterInfo.references.push_back(weaponRef);

    analysis.tables["Monster"] = monsterInfo;

    TableInfo weaponInfo;
    weaponInfo.name = "Weapon";
    weaponInfo.sourceFile = "test.fbs";
    weaponInfo.isImported = false;
    analysis.tables["Weapon"] = weaponInfo;

    JunctionTable jt;
    jt.name = "Monster__weapon";
    jt.parentTable = "Monster";
    jt.fieldName = "weapon";
    jt.relationType = RelationType::SINGLE_TABLE;
    jt.childTable = "Weapon";
    analysis.junctionTables.push_back(jt);

    // Initialize junction manager
    JunctionManager junctionMgr(db);
    junctionMgr.initialize(analysis);

    auto junctions = junctionMgr.getJunctionTables();
    assert(junctions.size() == 1);
    assert(junctions[0].name == "Monster__weapon");
    std::cout << "  Junction table created: " << junctions[0].name << std::endl;

    // Test junction table SQL generation
    std::string sql = jt.createSQL();
    std::cout << "  Generated SQL:" << std::endl;
    std::cout << "    " << sql.substr(0, sql.find('\n')) << "..." << std::endl;
    assert(sql.find("Monster__weapon") != std::string::npos);
    assert(sql.find("parent_rowid") != std::string::npos);
    assert(sql.find("child_rowid") != std::string::npos);

    std::cout << "Junction manager tests passed!" << std::endl;
}

int main() {
    std::cout << "=== FlatSQL Test Suite ===" << std::endl;
    std::cout << std::endl;

    try {
        testSchemaParser();
        testSQLParser();
        testBTree();
        testStorage();
        testDatabase();
        testSchemaAnalyzer();
        testCycleDetection();
        testJunctionManager();

        std::cout << std::endl;
        std::cout << "=== All tests passed! ===" << std::endl;
        return 0;
    } catch (const std::exception& e) {
        std::cerr << "Test failed with exception: " << e.what() << std::endl;
        return 1;
    }
}
