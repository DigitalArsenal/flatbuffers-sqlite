#pragma once

#include <string>
#include <vector>
#include <map>
#include <set>
#include <optional>
#include <functional>
#include <memory>

namespace flatsql {

// Forward declarations
class FlatSQLDatabase;

// Relationship types between tables
enum class RelationType {
    SINGLE_TABLE,      // field: OtherTable (0..1)
    VECTOR_TABLE,      // field: [OtherTable] (0..N)
    UNION,             // field: UnionType (0..1, polymorphic)
    VECTOR_UNION       // field: [UnionType] (0..N, polymorphic)
};

// Information about a field that references another table
struct TableReference {
    std::string fieldName;           // Name of the field in parent
    std::string referencedType;      // Table or union name
    RelationType relationType;
    std::vector<std::string> unionTypes;  // For unions: list of possible types
    int fieldId;                     // FlatBuffer field ID
};

// Information about a parsed table
struct TableInfo {
    std::string name;
    std::string sourceFile;          // Which .fbs file defined this
    std::vector<TableReference> references;  // Fields referencing other tables
    bool isImported;                 // Defined in an included file
    std::vector<std::string> indexedFields;  // Fields marked with (key) or (id)
};

// Information about a union type
struct UnionInfo {
    std::string name;
    std::vector<std::string> memberTypes;
    std::string sourceFile;
};

// Junction table definition
struct JunctionTable {
    std::string name;                // e.g., "Monster__weapons"
    std::string parentTable;
    std::string fieldName;
    RelationType relationType;

    // For non-union: single child table
    std::string childTable;

    // For union: multiple possible child tables
    std::vector<std::string> unionChildTables;

    // Generated SQL for the junction table
    std::string createSQL() const;
};

// Result of cycle detection
struct CycleInfo {
    bool hasCycle;
    std::vector<std::string> cyclePath;  // e.g., ["a.fbs", "b.fbs", "a.fbs"]
};

// Schema analysis result
struct SchemaAnalysis {
    std::map<std::string, TableInfo> tables;
    std::map<std::string, UnionInfo> unions;
    std::map<std::string, std::set<std::string>> importGraph;  // file -> imports
    std::vector<std::string> importOrder;  // Topological sort order
    std::vector<JunctionTable> junctionTables;
    std::optional<CycleInfo> cycle;
    std::vector<std::string> errors;
    std::vector<std::string> warnings;
};

// Junction row for linking parent to child
struct JunctionRow {
    uint64_t parentRowId;
    uint64_t childRowId;
    std::optional<int32_t> vectorIndex;   // For vector fields
    std::optional<std::string> unionType;  // For union fields
};

/**
 * Analyzes FlatBuffers schemas to extract relationship information
 * and generate junction table definitions.
 */
class SchemaAnalyzer {
public:
    SchemaAnalyzer();
    ~SchemaAnalyzer();

    // Parse a single schema file
    // filePath is used for import tracking, content is the .fbs content
    void addSchema(const std::string& filePath, const std::string& content);

    // Parse all added schemas and build analysis
    SchemaAnalysis analyze();

    // Check if a type is a struct (inline) vs table (needs junction)
    bool isStruct(const std::string& typeName) const;

private:
    struct Impl;
    std::unique_ptr<Impl> impl_;
};

/**
 * Manages junction tables and cascade operations for a FlatSQL database.
 */
class JunctionManager {
public:
    JunctionManager(FlatSQLDatabase& db);
    ~JunctionManager();

    // Initialize junction tables from schema analysis
    void initialize(const SchemaAnalysis& analysis);

    // Insert a FlatBuffer with automatic child extraction and junction creation
    // Returns the parent rowid
    uint64_t insertWithRelations(
        const std::string& tableName,
        const std::vector<uint8_t>& flatbufferData
    );

    // Delete a row with cascade to junction tables and orphan cleanup
    void deleteWithCascade(const std::string& tableName, uint64_t rowId);

    // Get all child rows for a parent
    std::vector<JunctionRow> getChildren(
        const std::string& parentTable,
        const std::string& fieldName,
        uint64_t parentRowId
    );

    // Get all parent rows that reference a child
    std::vector<JunctionRow> getParents(
        const std::string& childTable,
        uint64_t childRowId
    );

    // Clean up orphaned rows in a child table
    size_t cleanupOrphans(const std::string& tableName);

    // Get junction table info
    std::vector<JunctionTable> getJunctionTables() const;

    // Check reference count for a child row
    size_t getReferenceCount(const std::string& tableName, uint64_t rowId);

private:
    struct Impl;
    std::unique_ptr<Impl> impl_;
};

/**
 * Extracts nested FlatBuffers from a parent FlatBuffer.
 * Used during insert to split composite FlatBuffers into separate tables.
 */
class FlatBufferExtractor {
public:
    struct ExtractedChild {
        std::string fieldName;
        std::string tableName;
        std::vector<uint8_t> data;
        std::optional<int32_t> vectorIndex;
        std::optional<std::string> unionType;
    };

    // Extract all child FlatBuffers from a parent
    // Requires schema info to know which fields are table references
    static std::vector<ExtractedChild> extractChildren(
        const std::vector<uint8_t>& parentData,
        const TableInfo& tableInfo,
        const std::map<std::string, TableInfo>& allTables
    );
};

} // namespace flatsql
