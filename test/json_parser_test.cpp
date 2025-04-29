// test/json_parser_test.cpp
#include <gtest/gtest.h>
#include "json_parser.cpp"

TEST(JsonParserTest, TrailingCommaCrash) {
    std::string invalid_json = R"({
        "ALLOW_HTTP": "true",
        "AWS_ENDPOINT": "http://example.com",
    })"; // 尾部逗号
    EXPECT_THROW(parse_delta_options(invalid_json), std::runtime_error);
}

TEST(JsonParserTest, ValidJson) {
    std::string valid_json = R"({
        "ALLOW_HTTP": "true",
        "AWS_ENDPOINT": "http://example.com"
    })";
    EXPECT_NO_THROW(parse_delta_options(valid_json));
}