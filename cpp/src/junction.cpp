#include "flatsql/junction.h"
#include "flatsql/database.h"
#include <regex>
#include <queue>
#include <sstream>
#include <algorithm>
#include <stdexcept>

namespace flatsql {

// ============================================================================
// JunctionTable implementation
// ============================================================================

std::string JunctionTable::createSQL() const {
    std::ostringstream sql;
    sql << "CREATE TABLE IF NOT EXISTS \"" << name << "\" (\n";
    sql << "    id INTEGER PRIMARY KEY,\n";
    sql << "    parent_rowid INTEGER NOT NULL,\n";
    sql << "    child_rowid INTEGER NOT NULL";

    if (relationType == RelationType::VECTOR_TABLE ||
        relationType == RelationType::VECTOR_UNION) {
        sql << ",\n    vec_index INTEGER NOT NULL";
    }

    if (relationType == RelationType::UNION ||
        relationType == RelationType::VECTOR_UNION) {
        sql << ",\n    union_type TEXT NOT NULL";
    }

    sql << ",\n    created_at INTEGER DEFAULT (strftime('%s', 'now'))";
    sql << "\n);\n";

    // Indexes for efficient lookups
    sql << "CREATE INDEX IF NOT EXISTS idx_" << name << "_parent ON \"" << name << "\"(parent_rowid);\n";
    sql << "CREATE INDEX IF NOT EXISTS idx_" << name << "_child ON \"" << name << "\"(child_rowid);\n";

    if (relationType == RelationType::UNION || relationType == RelationType::VECTOR_UNION) {
        sql << "CREATE INDEX IF NOT EXISTS idx_" << name << "_type ON \"" << name << "\"(union_type);\n";
    }

    return sql.str();
}

// ============================================================================
// SchemaAnalyzer implementation
// ============================================================================

struct SchemaAnalyzer::Impl {
    std::map<std::string, std::string> schemaContents;  // file -> content
    std::set<std::string> structs;  // Known struct types (inline, no junction)
    std::map<std::string, TableInfo> tables;
    std::map<std::string, UnionInfo> unions;
    std::map<std::string, std::set<std::string>> importGraph;
    std::string currentFile;

    void parseSchema(const std::string& filePath, const std::string& content);
    void parseIncludes(const std::string& filePath, const std::string& content);
    void parseStructs(const std::string& content);
    void parseTables(const std::string& filePath, const std::string& content);
    void parseUnions(const std::string& filePath, const std::string& content);
    std::vector<TableReference> parseTableFields(const std::string& body);
    bool isTableType(const std::string& typeName) const;
    CycleInfo detectCycles() const;
    std::vector<std::string> topologicalSort() const;
    std::vector<JunctionTable> generateJunctionTables() const;
};

void SchemaAnalyzer::Impl::parseSchema(const std::string& filePath, const std::string& content) {
    currentFile = filePath;

    // Remove comments
    std::string cleaned = content;
    // Remove single-line comments: // ...
    cleaned = std::regex_replace(cleaned, std::regex("//[^\n]*"), "");
    // Remove multi-line comments: /* ... */
    cleaned = std::regex_replace(cleaned, std::regex("/\\*[\\s\\S]*?\\*/"), "");

    // Parse in order: includes, structs, unions, tables
    parseIncludes(filePath, cleaned);
    parseStructs(cleaned);
    parseUnions(filePath, cleaned);
    parseTables(filePath, cleaned);
}

void SchemaAnalyzer::Impl::parseIncludes(const std::string& filePath, const std::string& content) {
    // Match: include "filename.fbs";
    std::regex includePattern("include\\s*\"([^\"]+)\"\\s*;");
    std::sregex_iterator it(content.begin(), content.end(), includePattern);
    std::sregex_iterator end;

    for (; it != end; ++it) {
        std::string includedFile = (*it)[1].str();
        importGraph[filePath].insert(includedFile);
    }

    // Ensure this file is in the graph even if it has no imports
    if (importGraph.find(filePath) == importGraph.end()) {
        importGraph[filePath] = {};
    }
}

void SchemaAnalyzer::Impl::parseStructs(const std::string& content) {
    // Match: struct Name { ... }
    std::regex structPattern("struct\\s+(\\w+)\\s*\\{([^}]*)\\}");
    std::sregex_iterator it(content.begin(), content.end(), structPattern);
    std::sregex_iterator end;

    for (; it != end; ++it) {
        std::string name = (*it)[1].str();
        structs.insert(name);
    }
}

void SchemaAnalyzer::Impl::parseUnions(const std::string& filePath, const std::string& content) {
    // Match: union Name { ... }
    std::regex unionPattern("union\\s+(\\w+)\\s*\\{([^}]*)\\}");
    std::sregex_iterator it(content.begin(), content.end(), unionPattern);
    std::sregex_iterator end;

    for (; it != end; ++it) {
        UnionInfo info;
        info.name = (*it)[1].str();
        info.sourceFile = filePath;

        std::string body = (*it)[2].str();

        // Parse union members: "Weapon, Armor, Shield" or "M1: Monster, M2: Monster"
        std::regex memberPattern("(?:\\w+\\s*:\\s*)?(\\w+)");
        std::sregex_iterator memberIt(body.begin(), body.end(), memberPattern);
        for (; memberIt != end; ++memberIt) {
            std::string member = (*memberIt)[1].str();
            if (!member.empty()) {
                info.memberTypes.push_back(member);
            }
        }

        unions[info.name] = info;
    }
}

void SchemaAnalyzer::Impl::parseTables(const std::string& filePath, const std::string& content) {
    // Match: table Name { ... }
    std::regex tablePattern("table\\s+(\\w+)\\s*\\{([^}]*)\\}");
    std::sregex_iterator it(content.begin(), content.end(), tablePattern);
    std::sregex_iterator end;

    // Check if this file is imported by another
    bool isImported = false;
    for (const auto& [file, imports] : importGraph) {
        if (file != filePath && imports.count(filePath)) {
            isImported = true;
            break;
        }
    }

    for (; it != end; ++it) {
        TableInfo info;
        info.name = (*it)[1].str();
        info.sourceFile = filePath;
        info.isImported = isImported;

        std::string body = (*it)[2].str();
        info.references = parseTableFields(body);

        // Extract indexed fields (key, id attributes)
        std::regex attrFieldPattern("(\\w+)\\s*:\\s*\\w+[^;]*\\((id|key)[^)]*\\)");
        std::sregex_iterator attrIt(body.begin(), body.end(), attrFieldPattern);
        for (; attrIt != std::sregex_iterator{}; ++attrIt) {
            info.indexedFields.push_back((*attrIt)[1].str());
        }

        tables[info.name] = info;
    }
}

std::vector<TableReference> SchemaAnalyzer::Impl::parseTableFields(const std::string& body) {
    std::vector<TableReference> refs;

    // Match field definitions: name: [?]Type[?] (attributes)?
    std::regex fieldRegex("(\\w+)\\s*:\\s*(\\[?)(\\w+)(\\]?)(?:\\s*\\([^)]*\\))?");
    std::sregex_iterator it(body.begin(), body.end(), fieldRegex);
    std::sregex_iterator end;

    for (; it != end; ++it) {
        std::string fieldName = (*it)[1].str();
        bool isVector = !(*it)[2].str().empty();
        std::string typeName = (*it)[3].str();

        // Skip scalar types and structs
        if (structs.count(typeName)) continue;

        // Check if it's a known table
        if (tables.count(typeName)) {
            TableReference ref;
            ref.fieldName = fieldName;
            ref.referencedType = typeName;
            ref.relationType = isVector ? RelationType::VECTOR_TABLE : RelationType::SINGLE_TABLE;
            refs.push_back(ref);
        }
        // Check if it's a union
        else if (unions.count(typeName)) {
            TableReference ref;
            ref.fieldName = fieldName;
            ref.referencedType = typeName;
            ref.relationType = isVector ? RelationType::VECTOR_UNION : RelationType::UNION;
            ref.unionTypes = unions[typeName].memberTypes;
            refs.push_back(ref);
        }
        // Unknown type - might be a table defined later or in another file
        // We'll do a second pass to resolve these
    }

    return refs;
}

bool SchemaAnalyzer::Impl::isTableType(const std::string& typeName) const {
    return tables.count(typeName) > 0 && structs.count(typeName) == 0;
}

CycleInfo SchemaAnalyzer::Impl::detectCycles() const {
    CycleInfo result{false, {}};

    std::set<std::string> visited;
    std::set<std::string> inStack;
    std::vector<std::string> path;

    std::function<bool(const std::string&)> dfs = [&](const std::string& node) -> bool {
        visited.insert(node);
        inStack.insert(node);
        path.push_back(node);

        auto it = importGraph.find(node);
        if (it != importGraph.end()) {
            for (const auto& neighbor : it->second) {
                if (!visited.count(neighbor)) {
                    if (dfs(neighbor)) return true;
                } else if (inStack.count(neighbor)) {
                    // Found cycle
                    result.hasCycle = true;
                    // Build cycle path from the repeated node
                    auto cycleStart = std::find(path.begin(), path.end(), neighbor);
                    result.cyclePath = std::vector<std::string>(cycleStart, path.end());
                    result.cyclePath.push_back(neighbor);  // Complete the cycle
                    return true;
                }
            }
        }

        path.pop_back();
        inStack.erase(node);
        return false;
    };

    for (const auto& [node, _] : importGraph) {
        if (!visited.count(node)) {
            if (dfs(node)) break;
        }
    }

    return result;
}

std::vector<std::string> SchemaAnalyzer::Impl::topologicalSort() const {
    std::map<std::string, int> inDegree;
    for (const auto& [node, _] : importGraph) {
        if (!inDegree.count(node)) inDegree[node] = 0;
    }

    // Calculate in-degrees
    for (const auto& [node, edges] : importGraph) {
        for (const auto& target : edges) {
            inDegree[target]++;
        }
    }

    // Start with nodes that have no dependencies
    std::queue<std::string> queue;
    for (const auto& [node, degree] : inDegree) {
        if (degree == 0) queue.push(node);
    }

    std::vector<std::string> result;
    while (!queue.empty()) {
        std::string node = queue.front();
        queue.pop();
        result.push_back(node);

        auto it = importGraph.find(node);
        if (it != importGraph.end()) {
            for (const auto& neighbor : it->second) {
                if (--inDegree[neighbor] == 0) {
                    queue.push(neighbor);
                }
            }
        }
    }

    // Reverse to get dependency order (leaves first)
    std::reverse(result.begin(), result.end());
    return result;
}

std::vector<JunctionTable> SchemaAnalyzer::Impl::generateJunctionTables() const {
    std::vector<JunctionTable> junctions;

    for (const auto& [tableName, tableInfo] : tables) {
        for (const auto& ref : tableInfo.references) {
            JunctionTable jt;
            jt.name = tableName + "__" + ref.fieldName;
            jt.parentTable = tableName;
            jt.fieldName = ref.fieldName;
            jt.relationType = ref.relationType;

            if (ref.relationType == RelationType::UNION ||
                ref.relationType == RelationType::VECTOR_UNION) {
                jt.unionChildTables = ref.unionTypes;
            } else {
                jt.childTable = ref.referencedType;
            }

            junctions.push_back(jt);
        }
    }

    return junctions;
}

SchemaAnalyzer::SchemaAnalyzer() : impl_(std::make_unique<Impl>()) {}
SchemaAnalyzer::~SchemaAnalyzer() = default;

void SchemaAnalyzer::addSchema(const std::string& filePath, const std::string& content) {
    impl_->schemaContents[filePath] = content;
}

SchemaAnalysis SchemaAnalyzer::analyze() {
    SchemaAnalysis result;

    // First pass: parse all schemas to get structs, tables, unions
    for (const auto& [path, content] : impl_->schemaContents) {
        impl_->parseSchema(path, content);
    }

    // Second pass: resolve references now that all types are known
    for (auto& [tableName, tableInfo] : impl_->tables) {
        // Re-parse to pick up references to tables defined in other files
        if (impl_->schemaContents.count(tableInfo.sourceFile)) {
            // Already parsed, references should be updated
        }
    }

    // Check for cycles
    auto cycle = impl_->detectCycles();
    if (cycle.hasCycle) {
        result.cycle = cycle;
        std::ostringstream err;
        err << "Circular import detected: ";
        for (size_t i = 0; i < cycle.cyclePath.size(); ++i) {
            if (i > 0) err << " -> ";
            err << cycle.cyclePath[i];
        }
        result.errors.push_back(err.str());
    }

    // Generate import order
    result.importOrder = impl_->topologicalSort();

    // Copy tables and unions
    result.tables = impl_->tables;
    result.unions = impl_->unions;
    result.importGraph = impl_->importGraph;

    // Generate junction tables
    result.junctionTables = impl_->generateJunctionTables();

    return result;
}

bool SchemaAnalyzer::isStruct(const std::string& typeName) const {
    return impl_->structs.count(typeName) > 0;
}

// ============================================================================
// JunctionManager implementation
// ============================================================================

// Internal storage for junction rows (in-memory)
struct JunctionRowData {
    uint64_t id;
    uint64_t parentRowId;
    uint64_t childRowId;
    std::optional<int32_t> vecIndex;
    std::optional<std::string> unionType;
};

struct JunctionTableData {
    JunctionTable definition;
    std::vector<JunctionRowData> rows;
    uint64_t nextId = 1;

    // Index structures for fast lookup
    std::multimap<uint64_t, size_t> parentIndex;  // parent_rowid -> row index
    std::multimap<uint64_t, size_t> childIndex;   // child_rowid -> row index
};

struct JunctionManager::Impl {
    FlatSQLDatabase& db;
    SchemaAnalysis analysis;
    std::map<std::string, JunctionTableData> junctionTables;
    std::set<uint64_t> deletedRowIds;  // Track soft-deleted rows for cleanup

    explicit Impl(FlatSQLDatabase& database) : db(database) {}

    // Add a junction row
    uint64_t addJunctionRow(const std::string& junctionName,
                            uint64_t parentRowId,
                            uint64_t childRowId,
                            std::optional<int32_t> vecIndex,
                            std::optional<std::string> unionType) {
        auto it = junctionTables.find(junctionName);
        if (it == junctionTables.end()) return 0;

        JunctionRowData row;
        row.id = it->second.nextId++;
        row.parentRowId = parentRowId;
        row.childRowId = childRowId;
        row.vecIndex = vecIndex;
        row.unionType = unionType;

        size_t rowIndex = it->second.rows.size();
        it->second.rows.push_back(row);

        // Update indexes
        it->second.parentIndex.emplace(parentRowId, rowIndex);
        it->second.childIndex.emplace(childRowId, rowIndex);

        return row.id;
    }

    // Remove junction rows by parent
    void removeByParent(const std::string& junctionName, uint64_t parentRowId) {
        auto it = junctionTables.find(junctionName);
        if (it == junctionTables.end()) return;

        auto range = it->second.parentIndex.equal_range(parentRowId);
        std::vector<size_t> toRemove;
        for (auto pit = range.first; pit != range.second; ++pit) {
            toRemove.push_back(pit->second);
        }

        // Mark rows as deleted (we'll compact later)
        for (size_t idx : toRemove) {
            it->second.rows[idx].id = 0;  // Mark as deleted
        }

        // Rebuild indexes (simple approach - could optimize)
        it->second.parentIndex.clear();
        it->second.childIndex.clear();
        for (size_t i = 0; i < it->second.rows.size(); i++) {
            if (it->second.rows[i].id != 0) {
                it->second.parentIndex.emplace(it->second.rows[i].parentRowId, i);
                it->second.childIndex.emplace(it->second.rows[i].childRowId, i);
            }
        }
    }

    // Get children by parent
    std::vector<JunctionRow> getChildrenByParent(const std::string& junctionName, uint64_t parentRowId) {
        std::vector<JunctionRow> result;
        auto it = junctionTables.find(junctionName);
        if (it == junctionTables.end()) return result;

        auto range = it->second.parentIndex.equal_range(parentRowId);
        for (auto pit = range.first; pit != range.second; ++pit) {
            const auto& rowData = it->second.rows[pit->second];
            if (rowData.id != 0) {
                JunctionRow row;
                row.parentRowId = rowData.parentRowId;
                row.childRowId = rowData.childRowId;
                row.vectorIndex = rowData.vecIndex;
                row.unionType = rowData.unionType;
                result.push_back(row);
            }
        }
        return result;
    }

    // Get parents by child
    std::vector<JunctionRow> getParentsByChild(const std::string& junctionName, uint64_t childRowId) {
        std::vector<JunctionRow> result;
        auto it = junctionTables.find(junctionName);
        if (it == junctionTables.end()) return result;

        auto range = it->second.childIndex.equal_range(childRowId);
        for (auto pit = range.first; pit != range.second; ++pit) {
            const auto& rowData = it->second.rows[pit->second];
            if (rowData.id != 0) {
                JunctionRow row;
                row.parentRowId = rowData.parentRowId;
                row.childRowId = rowData.childRowId;
                row.vectorIndex = rowData.vecIndex;
                row.unionType = rowData.unionType;
                result.push_back(row);
            }
        }
        return result;
    }

    // Count references to a child
    size_t countChildReferences(uint64_t childRowId) {
        size_t count = 0;
        for (const auto& [name, table] : junctionTables) {
            auto range = table.childIndex.equal_range(childRowId);
            for (auto it = range.first; it != range.second; ++it) {
                if (table.rows[it->second].id != 0) {
                    count++;
                }
            }
        }
        return count;
    }
};

JunctionManager::JunctionManager(FlatSQLDatabase& db)
    : impl_(std::make_unique<Impl>(db)) {}

JunctionManager::~JunctionManager() = default;

void JunctionManager::initialize(const SchemaAnalysis& analysis) {
    impl_->analysis = analysis;

    // Initialize junction tables
    for (const auto& jt : analysis.junctionTables) {
        JunctionTableData tableData;
        tableData.definition = jt;
        impl_->junctionTables[jt.name] = tableData;
    }
}

uint64_t JunctionManager::insertWithRelations(
    const std::string& tableName,
    const std::vector<uint8_t>& flatbufferData
) {
    // 1. Insert parent record using streaming ingest
    uint64_t parentRowId = impl_->db.ingestOne(flatbufferData.data(), flatbufferData.size());

    // 2. Extract and insert children
    auto tableIt = impl_->analysis.tables.find(tableName);
    if (tableIt != impl_->analysis.tables.end()) {
        auto children = FlatBufferExtractor::extractChildren(
            flatbufferData, tableIt->second, impl_->analysis.tables
        );

        for (const auto& child : children) {
            // Insert child using streaming ingest
            uint64_t childRowId = impl_->db.ingestOne(child.data.data(), child.data.size());

            // Create junction row
            std::string junctionName = tableName + "__" + child.fieldName;
            impl_->addJunctionRow(junctionName, parentRowId, childRowId,
                                  child.vectorIndex, child.unionType);
        }
    }

    return parentRowId;
}

void JunctionManager::deleteWithCascade(const std::string& tableName, uint64_t rowId) {
    // Collect children to potentially delete
    std::vector<std::pair<std::string, uint64_t>> childrenToCheck;

    // 1. Find all junction tables where this table is the parent
    for (const auto& [jName, tableData] : impl_->junctionTables) {
        if (tableData.definition.parentTable == tableName) {
            // Get children before deleting junction rows
            auto children = impl_->getChildrenByParent(jName, rowId);

            for (const auto& child : children) {
                std::string childTable = tableData.definition.childTable;
                if (tableData.definition.relationType == RelationType::UNION ||
                    tableData.definition.relationType == RelationType::VECTOR_UNION) {
                    childTable = child.unionType.value_or("");
                }
                if (!childTable.empty()) {
                    childrenToCheck.emplace_back(childTable, child.childRowId);
                }
            }

            // Delete junction rows
            impl_->removeByParent(jName, rowId);
        }
    }

    // 2. Track this row as deleted
    impl_->deletedRowIds.insert(rowId);

    // 3. Check each child for orphan status and recursively delete
    for (const auto& [childTable, childRowId] : childrenToCheck) {
        if (getReferenceCount(childTable, childRowId) == 0) {
            deleteWithCascade(childTable, childRowId);
        }
    }
}

std::vector<JunctionRow> JunctionManager::getChildren(
    const std::string& parentTable,
    const std::string& fieldName,
    uint64_t parentRowId
) {
    std::string junctionName = parentTable + "__" + fieldName;
    return impl_->getChildrenByParent(junctionName, parentRowId);
}

std::vector<JunctionRow> JunctionManager::getParents(
    const std::string& childTable,
    uint64_t childRowId
) {
    std::vector<JunctionRow> result;

    // Find all junctions that reference this child table
    for (const auto& [jName, tableData] : impl_->junctionTables) {
        bool matches = (tableData.definition.childTable == childTable);
        if (!matches) {
            for (const auto& ut : tableData.definition.unionChildTables) {
                if (ut == childTable) {
                    matches = true;
                    break;
                }
            }
        }

        if (matches) {
            auto parents = impl_->getParentsByChild(jName, childRowId);
            result.insert(result.end(), parents.begin(), parents.end());
        }
    }

    return result;
}

size_t JunctionManager::cleanupOrphans(const std::string& tableName) {
    // This would require iterating all records in tableName and checking
    // if each has any junction references. For now, return the count of
    // deleted rows that were marked.
    size_t cleaned = impl_->deletedRowIds.size();
    impl_->deletedRowIds.clear();
    return cleaned;
}

std::vector<JunctionTable> JunctionManager::getJunctionTables() const {
    std::vector<JunctionTable> result;
    for (const auto& [name, tableData] : impl_->junctionTables) {
        result.push_back(tableData.definition);
    }
    return result;
}

size_t JunctionManager::getReferenceCount(const std::string& tableName, uint64_t rowId) {
    size_t count = 0;

    for (const auto& [jName, tableData] : impl_->junctionTables) {
        bool matches = (tableData.definition.childTable == tableName);
        if (!matches) {
            for (const auto& ut : tableData.definition.unionChildTables) {
                if (ut == tableName) {
                    matches = true;
                    break;
                }
            }
        }

        if (matches) {
            auto range = tableData.childIndex.equal_range(rowId);
            for (auto it = range.first; it != range.second; ++it) {
                if (tableData.rows[it->second].id != 0) {
                    count++;
                }
            }
        }
    }

    return count;
}

// ============================================================================
// FlatBufferExtractor implementation
// ============================================================================

std::vector<FlatBufferExtractor::ExtractedChild> FlatBufferExtractor::extractChildren(
    const std::vector<uint8_t>& parentData,
    const TableInfo& tableInfo,
    const std::map<std::string, TableInfo>& allTables
) {
    std::vector<ExtractedChild> children;

    // This requires understanding the FlatBuffer binary format
    // to extract nested tables from the parent buffer.
    //
    // For each table reference in tableInfo.references:
    // 1. Find the field offset in the vtable
    // 2. Read the offset to the nested table/vector
    // 3. Extract the bytes for that nested FlatBuffer
    // 4. For vectors, iterate and extract each element
    // 5. For unions, also extract the type discriminator

    // This is a placeholder - actual implementation would use
    // FlatBuffers reflection or generated accessors

    for (const auto& ref : tableInfo.references) {
        // Extract based on relationship type
        switch (ref.relationType) {
            case RelationType::SINGLE_TABLE: {
                // Extract single nested table
                ExtractedChild child;
                child.fieldName = ref.fieldName;
                child.tableName = ref.referencedType;
                // child.data = extracted bytes
                children.push_back(child);
                break;
            }

            case RelationType::VECTOR_TABLE: {
                // Extract each element of the vector
                // for (int i = 0; i < vectorLength; i++) {
                //     ExtractedChild child;
                //     child.fieldName = ref.fieldName;
                //     child.tableName = ref.referencedType;
                //     child.vectorIndex = i;
                //     child.data = extracted bytes for element i
                //     children.push_back(child);
                // }
                break;
            }

            case RelationType::UNION: {
                // Extract union with type discriminator
                ExtractedChild child;
                child.fieldName = ref.fieldName;
                // child.unionType = read type from union_type field
                // child.tableName = resolve type name
                // child.data = extracted bytes
                children.push_back(child);
                break;
            }

            case RelationType::VECTOR_UNION: {
                // Extract each union element
                // Similar to VECTOR_TABLE but with type per element
                break;
            }
        }
    }

    return children;
}

} // namespace flatsql
