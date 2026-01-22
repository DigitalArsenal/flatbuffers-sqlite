// Unix socket streaming test
// Demonstrates streaming raw FlatBuffers over a Unix domain socket

#include "flatsql/database.h"
#include "../schemas/test_schema_generated.h"
#include <iostream>
#include <cassert>
#include <cstring>
#include <thread>
#include <atomic>
#include <vector>
#include <chrono>

// Unix socket headers
#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>
#include <fcntl.h>

using namespace flatsql;

static const char* SOCKET_PATH = "/tmp/flatsql_test.sock";

// Helper: Create a User FlatBuffer with file identifier
std::vector<uint8_t> createUser(int32_t id, const char* name, const char* email, int32_t age) {
    flatbuffers::FlatBufferBuilder builder(256);
    auto user = test::CreateUserDirect(builder, id, name, email, age);
    builder.Finish(user, "USER");
    const uint8_t* buf = builder.GetBufferPointer();
    return std::vector<uint8_t>(buf, buf + builder.GetSize());
}

// Helper: Create a Post FlatBuffer with file identifier
std::vector<uint8_t> createPost(int32_t id, int32_t userId, const char* title, const char* content) {
    flatbuffers::FlatBufferBuilder builder(256);
    auto post = test::CreatePostDirect(builder, id, userId, title, content);
    builder.Finish(post, "POST");
    const uint8_t* buf = builder.GetBufferPointer();
    return std::vector<uint8_t>(buf, buf + builder.GetSize());
}

// Field extractor for User table
Value extractUserField(const uint8_t* data, size_t length, const std::string& fieldName) {
    (void)length;
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

// Build size-prefixed stream from multiple FlatBuffers
std::vector<uint8_t> buildStream(const std::vector<std::vector<uint8_t>>& buffers) {
    std::vector<uint8_t> stream;
    for (const auto& buf : buffers) {
        uint32_t size = static_cast<uint32_t>(buf.size());
        stream.push_back(static_cast<uint8_t>(size));
        stream.push_back(static_cast<uint8_t>(size >> 8));
        stream.push_back(static_cast<uint8_t>(size >> 16));
        stream.push_back(static_cast<uint8_t>(size >> 24));
        stream.insert(stream.end(), buf.begin(), buf.end());
    }
    return stream;
}

// Server: listens on Unix socket, ingests FlatBuffers, runs queries
class SocketServer {
public:
    std::atomic<bool> running{false};
    std::atomic<size_t> recordsIngested{0};
    FlatSQLDatabase* db = nullptr;

    void run() {
        // Remove existing socket
        unlink(SOCKET_PATH);

        // Create socket
        int serverFd = socket(AF_UNIX, SOCK_STREAM, 0);
        if (serverFd < 0) {
            std::cerr << "Server: Failed to create socket\n";
            return;
        }

        // Bind
        struct sockaddr_un addr;
        memset(&addr, 0, sizeof(addr));
        addr.sun_family = AF_UNIX;
        strncpy(addr.sun_path, SOCKET_PATH, sizeof(addr.sun_path) - 1);

        if (bind(serverFd, (struct sockaddr*)&addr, sizeof(addr)) < 0) {
            std::cerr << "Server: Failed to bind\n";
            close(serverFd);
            return;
        }

        // Listen
        if (listen(serverFd, 1) < 0) {
            std::cerr << "Server: Failed to listen\n";
            close(serverFd);
            return;
        }

        running = true;
        std::cerr << "Server: Listening on " << SOCKET_PATH << "\n";

        // Accept connection
        int clientFd = accept(serverFd, nullptr, nullptr);
        if (clientFd < 0) {
            std::cerr << "Server: Failed to accept\n";
            close(serverFd);
            return;
        }

        std::cerr << "Server: Client connected\n";

        // Create database
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

        FlatSQLDatabase database = FlatSQLDatabase::fromSchema(schema, "socket_test");
        database.registerFileId("USER", "User");
        database.registerFileId("POST", "Post");
        database.setFieldExtractor("User", extractUserField);
        database.setFieldExtractor("Post", extractPostField);
        db = &database;

        // Read and ingest from socket
        std::vector<uint8_t> buffer;
        char chunk[4096];

        while (true) {
            ssize_t bytesRead = read(clientFd, chunk, sizeof(chunk));
            if (bytesRead <= 0) {
                break;  // Connection closed or error
            }

            buffer.insert(buffer.end(), chunk, chunk + bytesRead);

            // Ingest complete records - returns bytes consumed
            size_t records = 0;
            size_t bytesConsumed = database.ingest(buffer.data(), buffer.size(), &records);
            if (records > 0) {
                recordsIngested += records;
                std::cerr << "Server: Ingested " << records << " records (total: " << recordsIngested << ")\n";

                // Remove consumed bytes from buffer
                buffer.erase(buffer.begin(), buffer.begin() + bytesConsumed);
            }
        }

        // Final stats
        auto stats = database.getStats();
        std::cerr << "Server: Final statistics:\n";
        for (const auto& s : stats) {
            std::cerr << "  " << s.tableName << " (" << s.fileId << "): " << s.recordCount << " records\n";
        }

        // Run test queries
        std::cerr << "Server: Running test queries...\n";

        auto userResult = database.query("SELECT * FROM User");
        std::cerr << "  SELECT * FROM User: " << userResult.rowCount() << " rows\n";

        auto postResult = database.query("SELECT * FROM Post");
        std::cerr << "  SELECT * FROM Post: " << postResult.rowCount() << " rows\n";

        close(clientFd);
        close(serverFd);
        unlink(SOCKET_PATH);

        running = false;
        std::cerr << "Server: Shutdown complete\n";
    }
};

// Client: connects to Unix socket and streams FlatBuffers
class SocketClient {
public:
    bool sendStream(const std::vector<uint8_t>& data) {
        // Wait for server to be ready
        std::this_thread::sleep_for(std::chrono::milliseconds(100));

        // Create socket
        int fd = socket(AF_UNIX, SOCK_STREAM, 0);
        if (fd < 0) {
            std::cerr << "Client: Failed to create socket\n";
            return false;
        }

        // Connect
        struct sockaddr_un addr;
        memset(&addr, 0, sizeof(addr));
        addr.sun_family = AF_UNIX;
        strncpy(addr.sun_path, SOCKET_PATH, sizeof(addr.sun_path) - 1);

        if (connect(fd, (struct sockaddr*)&addr, sizeof(addr)) < 0) {
            std::cerr << "Client: Failed to connect\n";
            close(fd);
            return false;
        }

        std::cerr << "Client: Connected to server\n";

        // Send data in chunks to simulate streaming
        size_t offset = 0;
        size_t chunkSize = 256;  // Small chunks to test streaming

        while (offset < data.size()) {
            size_t remaining = data.size() - offset;
            size_t toSend = std::min(chunkSize, remaining);

            ssize_t sent = write(fd, data.data() + offset, toSend);
            if (sent <= 0) {
                std::cerr << "Client: Failed to send\n";
                close(fd);
                return false;
            }

            offset += sent;
            std::cerr << "Client: Sent " << sent << " bytes (" << offset << "/" << data.size() << ")\n";

            // Small delay to simulate network latency
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }

        std::cerr << "Client: Stream complete, closing connection\n";
        close(fd);
        return true;
    }
};

void testBasicSocketStreaming() {
    std::cout << "Testing basic Unix socket streaming..." << std::endl;

    // Build test data: 10 users, 5 posts
    std::vector<std::vector<uint8_t>> flatbuffers;

    for (int i = 1; i <= 10; i++) {
        std::string name = "User" + std::to_string(i);
        std::string email = "user" + std::to_string(i) + "@test.com";
        flatbuffers.push_back(createUser(i, name.c_str(), email.c_str(), 20 + i));
    }

    for (int i = 1; i <= 5; i++) {
        std::string title = "Post " + std::to_string(i);
        std::string content = "Content for post " + std::to_string(i);
        flatbuffers.push_back(createPost(i, (i % 10) + 1, title.c_str(), content.c_str()));
    }

    std::vector<uint8_t> stream = buildStream(flatbuffers);
    std::cout << "  Built stream: " << stream.size() << " bytes, "
              << flatbuffers.size() << " FlatBuffers" << std::endl;

    // Start server in background thread
    SocketServer server;
    std::thread serverThread([&server]() {
        server.run();
    });

    // Wait for server to start
    while (!server.running) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }

    // Client sends stream
    SocketClient client;
    bool success = client.sendStream(stream);
    assert(success);

    // Wait for server to finish
    serverThread.join();

    // Verify results
    assert(server.recordsIngested == 15);

    std::cout << "Basic socket streaming test passed!" << std::endl;
}

void testLargeStreamingBatch() {
    std::cout << "Testing large streaming batch over Unix socket..." << std::endl;

    // Build larger test data: 1000 users
    std::vector<std::vector<uint8_t>> flatbuffers;

    for (int i = 1; i <= 1000; i++) {
        std::string name = "BatchUser" + std::to_string(i);
        std::string email = "batch" + std::to_string(i) + "@example.com";
        flatbuffers.push_back(createUser(i, name.c_str(), email.c_str(), 18 + (i % 60)));
    }

    std::vector<uint8_t> stream = buildStream(flatbuffers);
    std::cout << "  Built stream: " << stream.size() << " bytes, "
              << flatbuffers.size() << " FlatBuffers" << std::endl;

    // Start server
    SocketServer server;
    std::thread serverThread([&server]() {
        server.run();
    });

    while (!server.running) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }

    // Client sends stream
    SocketClient client;
    bool success = client.sendStream(stream);
    assert(success);

    serverThread.join();

    // Verify
    assert(server.recordsIngested == 1000);

    std::cout << "Large streaming batch test passed!" << std::endl;
}

void testIncrementalStreaming() {
    std::cout << "Testing incremental streaming with queries..." << std::endl;

    // This test demonstrates that queries work correctly after partial ingestion
    std::string schema = R"(
        table User {
            id: int (id);
            name: string;
            email: string (key);
            age: int;
        }
    )";

    FlatSQLDatabase db = FlatSQLDatabase::fromSchema(schema, "incremental_test");
    db.registerFileId("USER", "User");
    db.setFieldExtractor("User", extractUserField);

    // Ingest in batches, querying between each batch
    for (int batch = 0; batch < 5; batch++) {
        std::vector<std::vector<uint8_t>> batchData;

        for (int i = 0; i < 20; i++) {
            int id = batch * 20 + i + 1;
            std::string name = "User" + std::to_string(id);
            std::string email = "user" + std::to_string(id) + "@test.com";
            batchData.push_back(createUser(id, name.c_str(), email.c_str(), 25));
        }

        std::vector<uint8_t> stream = buildStream(batchData);
        size_t records = 0;
        db.ingest(stream.data(), stream.size(), &records);

        assert(records == 20);

        // Query after each batch
        auto result = db.query("SELECT * FROM User");
        size_t expectedCount = (batch + 1) * 20;
        assert(result.rowCount() == expectedCount);

        std::cout << "  Batch " << (batch + 1) << ": ingested 20, total " << result.rowCount() << std::endl;
    }

    // Final indexed query
    auto result = db.query("SELECT * FROM User WHERE id = 50");
    assert(result.rowCount() == 1);

    std::cout << "Incremental streaming test passed!" << std::endl;
}

void testExportReloadCycle() {
    std::cout << "Testing export/reload cycle with streaming..." << std::endl;

    std::string schema = R"(
        table User {
            id: int (id);
            name: string;
            email: string (key);
            age: int;
        }
    )";

    // Phase 1: Create and populate
    FlatSQLDatabase db1 = FlatSQLDatabase::fromSchema(schema, "export_test");
    db1.registerFileId("USER", "User");
    db1.setFieldExtractor("User", extractUserField);

    std::vector<std::vector<uint8_t>> users;
    for (int i = 1; i <= 50; i++) {
        std::string name = "ExportUser" + std::to_string(i);
        std::string email = "export" + std::to_string(i) + "@test.com";
        users.push_back(createUser(i, name.c_str(), email.c_str(), 30));
    }

    std::vector<uint8_t> stream = buildStream(users);
    db1.ingest(stream.data(), stream.size());

    auto result1 = db1.query("SELECT * FROM User");
    assert(result1.rowCount() == 50);

    // Export
    std::vector<uint8_t> exported = db1.exportData();
    std::cout << "  Exported: " << exported.size() << " bytes" << std::endl;

    // Phase 2: Reload into new database
    FlatSQLDatabase db2 = FlatSQLDatabase::fromSchema(schema, "reload_test");
    db2.registerFileId("USER", "User");
    db2.setFieldExtractor("User", extractUserField);
    db2.loadAndRebuild(exported.data(), exported.size());

    auto result2 = db2.query("SELECT * FROM User");
    assert(result2.rowCount() == 50);

    // Verify indexed query works after reload
    auto indexed = db2.query("SELECT * FROM User WHERE email = 'export25@test.com'");
    assert(indexed.rowCount() == 1);

    std::cout << "  Reloaded: " << result2.rowCount() << " records, indexes working" << std::endl;

    std::cout << "Export/reload cycle test passed!" << std::endl;
}

void testMixedTableStreaming() {
    std::cout << "Testing mixed table streaming (interleaved types)..." << std::endl;

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

    FlatSQLDatabase db = FlatSQLDatabase::fromSchema(schema, "mixed_test");
    db.registerFileId("USER", "User");
    db.registerFileId("POST", "Post");
    db.setFieldExtractor("User", extractUserField);
    db.setFieldExtractor("Post", extractPostField);

    // Interleave users and posts in the stream
    std::vector<std::vector<uint8_t>> mixed;

    for (int i = 1; i <= 10; i++) {
        // Add user
        std::string name = "User" + std::to_string(i);
        std::string email = "user" + std::to_string(i) + "@test.com";
        mixed.push_back(createUser(i, name.c_str(), email.c_str(), 25));

        // Add 2 posts for this user
        for (int j = 1; j <= 2; j++) {
            int postId = (i - 1) * 2 + j;
            std::string title = "Post " + std::to_string(postId) + " by User " + std::to_string(i);
            std::string content = "Content " + std::to_string(postId);
            mixed.push_back(createPost(postId, i, title.c_str(), content.c_str()));
        }
    }

    std::vector<uint8_t> stream = buildStream(mixed);
    std::cout << "  Stream: " << stream.size() << " bytes, " << mixed.size() << " records" << std::endl;

    // Ingest all at once
    size_t records = 0;
    db.ingest(stream.data(), stream.size(), &records);
    assert(records == 30);  // 10 users + 20 posts

    // Verify
    auto users = db.query("SELECT * FROM User");
    assert(users.rowCount() == 10);

    auto posts = db.query("SELECT * FROM Post");
    assert(posts.rowCount() == 20);

    // Verify indexed query across tables
    auto user5Posts = db.query("SELECT * FROM Post WHERE user_id = 5");
    assert(user5Posts.rowCount() == 2);

    std::cout << "  Users: " << users.rowCount() << ", Posts: " << posts.rowCount() << std::endl;

    std::cout << "Mixed table streaming test passed!" << std::endl;
}

int main() {
    std::cout << "=== FlatSQL Socket Streaming Tests ===" << std::endl;
    std::cout << "Testing raw FlatBuffer streaming over Unix sockets" << std::endl;
    std::cout << std::endl;

    try {
        // In-memory streaming tests (no socket)
        testIncrementalStreaming();
        testExportReloadCycle();
        testMixedTableStreaming();

        // Unix socket streaming tests
        testBasicSocketStreaming();
        testLargeStreamingBatch();

        std::cout << std::endl;
        std::cout << "=== All socket streaming tests passed! ===" << std::endl;
        return 0;
    } catch (const std::exception& e) {
        std::cerr << "Test failed with exception: " << e.what() << std::endl;
        return 1;
    }
}
