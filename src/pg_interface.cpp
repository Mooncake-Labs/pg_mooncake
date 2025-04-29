// src/pg_interface.cpp
extern "C" {
    #include "postgres.h"
    #include "utils/builtins.h"
}

void handle_delta_options(const char* json_str) {
    try {
        parse_delta_options(json_str);
    } catch (const std::exception& e) {
        ereport(ERROR, 
            (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
            (errmsg("Failed to parse delta_options: %s", e.what()))
        );
    }
}