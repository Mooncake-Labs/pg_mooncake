// src/json_parser.cpp
#include <rapidjson/document.h>
#include <rapidjson/error/en.h>
#include <stdexcept>

void parse_delta_options(const std::string& json_str) {
    rapidjson::Document doc;
    doc.Parse(json_str.c_str());

    if (doc.HasParseError()) {
        // 获取错误信息
        const char* error_msg = rapidjson::GetParseError_En(doc.GetParseError());
        throw std::runtime_error("Invalid JSON in delta_options: " + std::string(error_msg));
    }

    // 其他处理逻辑...
}