// Integration test using real FlatBuffer generated code
// Tests: create → stream → query → retrieve → verify

#include "flatsql/database.h"
#include "../schemas/test_schema_generated.h"
#include <iostream>
#include <cassert>
#include <cstring>

using namespace flatsql;

// Helper: Create a User FlatBuffer with file identifier
std::vector<uint8_t> createUserFlatBuffer(int32_t id, const char* name, const char* email, int32_t age) {
    flatbuffers::FlatBufferBuilder builder(256);
    auto user = test::CreateUserDirect(builder, id, name, email, age);
    // Finish with file identifier "USER"
    builder.Finish(user, "USER");

    const uint8_t* buf = builder.GetBufferPointer();
    size_t size = builder.GetSize();
    return std::vector<uint8_t>(buf, buf + size);
}

// Helper: Create a Post FlatBuffer with file identifier
std::vector<uint8_t> createPostFlatBuffer(int32_t id, int32_t userId, const char* title, const char* content) {
    flatbuffers::FlatBufferBuilder builder(256);
    auto post = test::CreatePostDirect(builder, id, userId, title, content);
    // Finish with file identifier "POST"
    builder.Finish(post, "POST");

    const uint8_t* buf = builder.GetBufferPointer();
    size_t size = builder.GetSize();
    return std::vector<uint8_t>(buf, buf + size);
}

// Field extractor for User table (new signature: raw pointer + length)
Value extractUserField(const uint8_t* data, size_t length, const std::string& fieldName) {
    (void)length;  // Unused for now
    auto user = test::GetUser(data);
    if (!user) return std::monostate{};

    if (fieldName == "id") return user->id();
    if (fieldName == "name") return user->name() ? std::string(user->name()->c_str()) : std::string();
    if (fieldName == "email") return user->email() ? std::string(user->email()->c_str()) : std::string();
    if (fieldName == "age") return user->age();

    return std::monostate{};
}

// Field extractor for Post table
Value extractPostField(const uint8_t* data, size_t length, const std::string& fieldName) {
    (void)length;
    auto post = flatbuffers::GetRoot<test::Post>(data);
    if (!post) return std::monostate{};

    if (fieldName == "id") return post->id();
    if (fieldName == "user_id") return post->user_id();
    if (fieldName == "title") return post->title() ? std::string(post->title()->c_str()) : std::string();
    if (fieldName == "content") return post->content() ? std::string(post->content()->c_str()) : std::string();

    return std::monostate{};
}

void testFlatBufferCreation() {
    std::cout << "Testing FlatBuffer creation..." << std::endl;

    // Create a User FlatBuffer
    auto userData = createUserFlatBuffer(1, "Alice", "alice@example.com", 30);
    assert(userData.size() > 0);

    // Verify file identifier is set (bytes 4-7)
    assert(userData.size() >= 8);
    std::string fileId(reinterpret_cast<const char*>(userData.data() + 4), 4);
    assert(fileId == "USER");
    std::cout << "  File identifier: " << fileId << std::endl;

    // Verify we can read it back
    auto user = test::GetUser(userData.data());
    assert(user != nullptr);
    assert(user->id() == 1);
    assert(std::strcmp(user->name()->c_str(), "Alice") == 0);
    assert(std::strcmp(user->email()->c_str(), "alice@example.com") == 0);
    assert(user->age() == 30);

    std::cout << "  Created User: id=" << user->id()
              << ", name=" << user->name()->c_str()
              << ", email=" << user->email()->c_str()
              << ", age=" << user->age() << std::endl;

    std::cout << "FlatBuffer creation test passed!" << std::endl;
}

void testStoreAndRetrieve() {
    std::cout << "Testing store and retrieve with real FlatBuffers..." << std::endl;

    // Create schema matching our test FlatBuffers
    std::string schema = R"(
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

    auto db = FlatSQLDatabase::fromSchema(schema, "integration_test");

    // Register file identifiers for routing
    db.registerFileId("USER", "User");
    db.registerFileId("POST", "Post");

    // Set field extractors
    db.setFieldExtractor("User", extractUserField);
    db.setFieldExtractor("Post", extractPostField);

    // Create and ingest multiple users
    std::cout << "  Ingesting users..." << std::endl;
    auto user1 = createUserFlatBuffer(1, "Alice", "alice@example.com", 30);
    auto user2 = createUserFlatBuffer(2, "Bob", "bob@example.com", 25);
    auto user3 = createUserFlatBuffer(3, "Charlie", "charlie@example.com", 35);
    db.ingestOne(user1.data(), user1.size());
    db.ingestOne(user2.data(), user2.size());
    db.ingestOne(user3.data(), user3.size());

    // Create and ingest posts
    std::cout << "  Ingesting posts..." << std::endl;
    auto post1 = createPostFlatBuffer(1, 1, "Hello World", "My first post");
    auto post2 = createPostFlatBuffer(2, 1, "FlatBuffers Rock", "Using FlatBuffers with SQL");
    auto post3 = createPostFlatBuffer(3, 2, "Bob's Post", "This is Bob's content");
    db.ingestOne(post1.data(), post1.size());
    db.ingestOne(post2.data(), post2.size());
    db.ingestOne(post3.data(), post3.size());

    // Query all users (use explicit columns to avoid virtual columns)
    std::cout << "  Querying SELECT id, name, email, age FROM User..." << std::endl;
    auto result = db.query("SELECT id, name, email, age FROM User");

    assert(result.rowCount() == 3);
    std::cout << "    Found " << result.rowCount() << " users" << std::endl;

    // Verify columns
    assert(result.columns.size() == 4);
    assert(result.columns[0] == "id");
    assert(result.columns[1] == "name");
    assert(result.columns[2] == "email");
    assert(result.columns[3] == "age");

    // Query posts for user 1
    std::cout << "  Querying SELECT * FROM Post WHERE user_id = 1..." << std::endl;
    auto postResult = db.query("SELECT * FROM Post WHERE user_id = 1");
    std::cout << "    Found " << postResult.rowCount() << " posts for user 1" << std::endl;
    assert(postResult.rowCount() == 2);

    std::cout << "Store and retrieve test passed!" << std::endl;
}

void testStreamingIngest() {
    std::cout << "Testing streaming ingest..." << std::endl;

    std::string schema = R"(
        table User {
            id: int (id);
            name: string;
            email: string (key);
            age: int;
        }
    )";

    auto db = FlatSQLDatabase::fromSchema(schema, "stream_test");
    db.registerFileId("USER", "User");
    db.setFieldExtractor("User", extractUserField);

    // Create and ingest batch of FlatBuffers
    std::cout << "  Ingesting 100 users..." << std::endl;
    for (int i = 0; i < 100; i++) {
        std::string name = "User" + std::to_string(i);
        std::string email = "user" + std::to_string(i) + "@example.com";
        auto userData = createUserFlatBuffer(i, name.c_str(), email.c_str(), 20 + i);
        db.ingestOne(userData.data(), userData.size());
    }

    // Verify count
    auto result = db.query("SELECT * FROM User");
    assert(result.rowCount() == 100);
    std::cout << "  Verified 100 users in database" << std::endl;

    std::cout << "Streaming ingest test passed!" << std::endl;
}

void testExportAndReload() {
    std::cout << "Testing export and reload..." << std::endl;

    std::string schema = R"(
        table User {
            id: int (id);
            name: string;
            email: string;
            age: int;
        }
    )";

    // Create and populate
    auto db1 = FlatSQLDatabase::fromSchema(schema, "export_test");
    db1.registerFileId("USER", "User");
    db1.setFieldExtractor("User", extractUserField);

    auto user1 = createUserFlatBuffer(1, "Alice", "alice@example.com", 30);
    auto user2 = createUserFlatBuffer(2, "Bob", "bob@example.com", 25);
    db1.ingestOne(user1.data(), user1.size());
    db1.ingestOne(user2.data(), user2.size());

    // Export
    auto exportedData = db1.exportData();
    std::cout << "  Exported " << exportedData.size() << " bytes" << std::endl;

    // The format is now: [4-byte size][FlatBuffer][4-byte size][FlatBuffer]...
    // First 4 bytes should be the size of the first FlatBuffer
    assert(exportedData.size() >= 4);

    // Reload into new database and verify records via streaming callback
    auto db2 = FlatSQLDatabase::fromSchema(schema, "reload_test");
    db2.registerFileId("USER", "User");
    db2.setFieldExtractor("User", extractUserField);
    db2.loadAndRebuild(exportedData.data(), exportedData.size());

    auto result = db2.query("SELECT * FROM User");
    assert(result.rowCount() == 2);
    std::cout << "  Reloaded " << result.rowCount() << " records" << std::endl;

    std::cout << "Export and reload test passed!" << std::endl;
}

void testIndexedQuery() {
    std::cout << "Testing indexed queries..." << std::endl;

    std::string schema = R"(
        table User {
            id: int (id);
            name: string;
            email: string (key);
            age: int;
        }
    )";

    auto db = FlatSQLDatabase::fromSchema(schema, "index_test");
    db.registerFileId("USER", "User");
    db.setFieldExtractor("User", extractUserField);

    // Ingest users
    for (int i = 0; i < 50; i++) {
        std::string name = "User" + std::to_string(i);
        std::string email = "user" + std::to_string(i) + "@test.com";
        auto userData = createUserFlatBuffer(i, name.c_str(), email.c_str(), 20 + i);
        db.ingestOne(userData.data(), userData.size());
    }

    // Query by indexed field (email is marked with 'key')
    std::cout << "  Querying by indexed email..." << std::endl;
    auto result = db.query("SELECT * FROM User WHERE email = 'user25@test.com'");
    std::cout << "    Found " << result.rowCount() << " matching record(s)" << std::endl;

    // Query by indexed field (id is marked with 'id')
    std::cout << "  Querying by indexed id..." << std::endl;
    result = db.query("SELECT * FROM User WHERE id = 30");
    std::cout << "    Found " << result.rowCount() << " matching record(s)" << std::endl;

    std::cout << "Indexed query test passed!" << std::endl;
}

void testMultiSource() {
    std::cout << "Testing multi-source routing..." << std::endl;

    std::string schema = R"(
        table User {
            id: int (id);
            name: string;
            email: string (key);
            age: int;
        }
    )";

    auto db = FlatSQLDatabase::fromSchema(schema, "multisource_test");

    // First set up file IDs and extractors on base tables
    db.registerFileId("USER", "User");
    db.setFieldExtractor("User", extractUserField);

    // Now register sources (they will copy file ID and extractor from base)
    std::cout << "  Registering sources..." << std::endl;
    db.registerSource("satellite-1");
    db.registerSource("satellite-2");
    db.registerSource("ground-station");

    // List sources
    auto sources = db.listSources();
    std::cout << "  Registered " << sources.size() << " sources:";
    for (const auto& s : sources) {
        std::cout << " " << s;
    }
    std::cout << std::endl;
    assert(sources.size() == 3);

    // Create unified views
    std::cout << "  Creating unified views..." << std::endl;
    db.createUnifiedViews();

    // Ingest data to each source
    std::cout << "  Ingesting data to satellite-1..." << std::endl;
    for (int i = 0; i < 3; i++) {
        std::string name = "Sat1User" + std::to_string(i);
        std::string email = "sat1_" + std::to_string(i) + "@space.com";
        auto userData = createUserFlatBuffer(i, name.c_str(), email.c_str(), 25 + i);
        db.ingestOneWithSource(userData.data(), userData.size(), "satellite-1");
    }

    std::cout << "  Ingesting data to satellite-2..." << std::endl;
    for (int i = 0; i < 2; i++) {
        std::string name = "Sat2User" + std::to_string(i);
        std::string email = "sat2_" + std::to_string(i) + "@space.com";
        auto userData = createUserFlatBuffer(100 + i, name.c_str(), email.c_str(), 30 + i);
        db.ingestOneWithSource(userData.data(), userData.size(), "satellite-2");
    }

    std::cout << "  Ingesting data to ground-station..." << std::endl;
    for (int i = 0; i < 4; i++) {
        std::string name = "GroundUser" + std::to_string(i);
        std::string email = "ground_" + std::to_string(i) + "@earth.com";
        auto userData = createUserFlatBuffer(200 + i, name.c_str(), email.c_str(), 40 + i);
        db.ingestOneWithSource(userData.data(), userData.size(), "ground-station");
    }

    // Query source-specific table
    std::cout << "  Querying User@satellite-1..." << std::endl;
    auto result = db.query("SELECT id, name FROM \"User@satellite-1\"");
    std::cout << "    Found " << result.rowCount() << " rows" << std::endl;
    assert(result.rowCount() == 3);

    std::cout << "  Querying User@satellite-2..." << std::endl;
    result = db.query("SELECT id, name FROM \"User@satellite-2\"");
    std::cout << "    Found " << result.rowCount() << " rows" << std::endl;
    assert(result.rowCount() == 2);

    std::cout << "  Querying User@ground-station..." << std::endl;
    result = db.query("SELECT id, name FROM \"User@ground-station\"");
    std::cout << "    Found " << result.rowCount() << " rows" << std::endl;
    assert(result.rowCount() == 4);

    // Query unified view (combines all sources)
    std::cout << "  Querying unified User view..." << std::endl;
    result = db.query("SELECT _source, id, name FROM User");
    std::cout << "    Found " << result.rowCount() << " total rows across all sources" << std::endl;
    assert(result.rowCount() == 9);  // 3 + 2 + 4

    // Print results
    std::cout << "    Results:" << std::endl;
    for (size_t i = 0; i < result.rowCount() && i < 5; i++) {
        std::string source = std::get<std::string>(result.rows[i][0]);
        int64_t id = std::get<int64_t>(result.rows[i][1]);
        std::string name = std::get<std::string>(result.rows[i][2]);
        std::cout << "      " << source << " | " << id << " | " << name << std::endl;
    }
    if (result.rowCount() > 5) {
        std::cout << "      ... and " << (result.rowCount() - 5) << " more rows" << std::endl;
    }

    std::cout << "Multi-source routing test passed!" << std::endl;
}

void testBatchStreamIngest() {
    std::cout << "Testing batch stream ingest (size-prefixed format)..." << std::endl;

    std::string schema = R"(
        table User {
            id: int (id);
            name: string;
            email: string;
            age: int;
        }
    )";

    auto db = FlatSQLDatabase::fromSchema(schema, "batch_test");
    db.registerFileId("USER", "User");
    db.setFieldExtractor("User", extractUserField);

    // Build a batch of size-prefixed FlatBuffers
    std::vector<uint8_t> batchData;
    for (int i = 0; i < 10; i++) {
        std::string name = "BatchUser" + std::to_string(i);
        std::string email = "batch" + std::to_string(i) + "@test.com";
        auto userData = createUserFlatBuffer(i, name.c_str(), email.c_str(), 30 + i);

        // Prepend size (little-endian)
        uint32_t size = static_cast<uint32_t>(userData.size());
        batchData.push_back(static_cast<uint8_t>(size));
        batchData.push_back(static_cast<uint8_t>(size >> 8));
        batchData.push_back(static_cast<uint8_t>(size >> 16));
        batchData.push_back(static_cast<uint8_t>(size >> 24));

        // Append FlatBuffer data
        batchData.insert(batchData.end(), userData.begin(), userData.end());
    }

    std::cout << "  Created batch of " << batchData.size() << " bytes" << std::endl;

    // Ingest the entire batch in one call
    size_t records = 0;
    size_t bytesConsumed = db.ingest(batchData.data(), batchData.size(), &records);
    std::cout << "  Ingested " << records << " records from batch (" << bytesConsumed << " bytes)" << std::endl;
    assert(records == 10);

    // Verify
    auto result = db.query("SELECT * FROM User");
    assert(result.rowCount() == 10);

    std::cout << "Batch stream ingest test passed!" << std::endl;
}

int main() {
    std::cout << "=== FlatSQL Integration Tests ===" << std::endl;
    std::cout << "Using streaming FlatBuffer ingest with file identifiers" << std::endl;
    std::cout << std::endl;

    try {
        testFlatBufferCreation();
        testStoreAndRetrieve();
        testStreamingIngest();
        testExportAndReload();
        testIndexedQuery();
        testBatchStreamIngest();
        testMultiSource();

        std::cout << std::endl;
        std::cout << "=== All integration tests passed! ===" << std::endl;
        return 0;
    } catch (const std::exception& e) {
        std::cerr << "Test failed with exception: " << e.what() << std::endl;
        return 1;
    }
}
