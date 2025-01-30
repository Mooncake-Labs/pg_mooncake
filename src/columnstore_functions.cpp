#include "duckdb.hpp"
#include <string>
#include <unordered_set>

extern "C" {
#include "postgres.h"
#include "executor/spi.h"
#include "fmgr.h"
#include "utils/builtins.h"
#include "utils/jsonb.h"
}

#include "pgduckdb/utility/cpp_wrapper.hpp"

constexpr int x_secrets_natts = 5;

std::string GetStringFromText(text *t) {
    std::string str_val(VARDATA_ANY(t), VARSIZE_ANY_EXHDR(t));
    return str_val;
}

std::string GetStringFromJsonbValue(JsonbValue jval) {
    std::string str_val(jval.val.string.val, jval.val.string.len);
    return str_val;
}

extern "C" {

DECLARE_PG_FUNCTION(create_secret) {
    const std::string name = GetStringFromText(PG_GETARG_TEXT_PP(0));
    const std::string type = GetStringFromText(PG_GETARG_TEXT_PP(1));
    const std::string key_id = GetStringFromText(PG_GETARG_TEXT_PP(2));
    const std::string secret = GetStringFromText(PG_GETARG_TEXT_PP(3));
    Jsonb *extra_params = PG_GETARG_JSONB_P(4);

    static const std::unordered_set<std::string> allowed_keys = {"ENDPOINT", "REGION", "SCOPE", "URL_STYLE", "USE_SSL"};

    JsonbContainer *conatiner = &extra_params->root;
    JsonbIterator *it = JsonbIteratorInit(conatiner);
    JsonbValue key;
    JsonbIteratorToken ret;

    if (type == "S3") {
        while ((ret = JsonbIteratorNext(&it, &key, false)) != WJB_DONE) {
            if (ret == WJB_KEY) {
                std::string key_str = GetStringFromJsonbValue(key);
                if (allowed_keys.find(key_str) == allowed_keys.end()) {
                    elog(ERROR, "Invalid extra parameter: %s\nAllowed parameters are ENDPOINT, REGION, SCOPE, URL_STYLE, USE_SSL.", \
                                key_str.c_str());
                }
            }
        }

        bool use_ssl = true;
        std::string delta_endpoint;
        JsonbValue val;

        if (getKeyJsonValueFromContainer(conatiner, "USE_SSL", 7, &val)) {
            std::string ssl_val = GetStringFromJsonbValue(val);
            use_ssl = !(ssl_val == "FALSE" || ssl_val == "false");
        }

        if (getKeyJsonValueFromContainer(conatiner, "ENDPOINT", 8, &val)) {
            std::string endpoint = GetStringFromJsonbValue(val);
            if (endpoint.find("://") != std::string::npos) {
                elog(ERROR, "Invalid ENDPOINT format: %s\nUse domain name excluding http prefix", \
                            endpoint.c_str());
            }
            if (!endpoint.empty() && endpoint.substr(0, 9) != "s3express") {
                delta_endpoint = use_ssl ? "https://" : "https:/";
                delta_endpoint += endpoint;
            }
        }

        if (getKeyJsonValueFromContainer(conatiner, "URL_STYLE", 9, &val)) {
            std::string url_style = GetStringFromJsonbValue(val);
            if (url_style != "path" && url_style != "vhost") {
                elog(ERROR, "Invalid URL_STYLE: %s\nAllowed values for URL_STYLE are \"path\" or \"vhost\".", \
                            url_style.c_str());
            }
        }

        std::string scope;
        if (getKeyJsonValueFromContainer(conatiner, "SCOPE", 5, &val)) {
            scope = GetStringFromJsonbValue(val);
        }

        std::string duckdb_query = "CREATE SECRET \"duckdb_secret_" + name + "\" (TYPE " + type + ", KEY_ID '" +
                                   key_id + "', SECRET '" + secret + "'";

        // "ENDPOINT", "REGION", "SCOPE", "URL_STYLE", "USE_SSL"
        for (const auto &key : allowed_keys) {
            if (getKeyJsonValueFromContainer(conatiner, key.c_str(), key.length(), &val)) {
                if (key == "USE_SSL") {
                    duckdb_query += ", USE_SSL '" + std::string(use_ssl ? "TRUE" : "FALSE") + "'";   
                } else {
                    duckdb_query += ", " + key + " '" + GetStringFromJsonbValue(val) + "'";
                }
            }
        }
        duckdb_query += ");";

        std::string delta_options = "{\"AWS_ACCESS_KEY_ID\": \"" + key_id + "\"" +
                                    ", \"AWS_SECRET_ACCESS_KEY\": \"" + secret + "\"";

        if (delta_endpoint.length() > 0) {
            delta_options += ", \"AWS_ENDPOINT\": \"" + delta_endpoint + "\"";
        }

        if (getKeyJsonValueFromContainer(conatiner, "REGION", 6, &val)) {
            delta_options += ", \"AWS_REGION\": \"" + GetStringFromJsonbValue(val) + "\"";
        }

        if (getKeyJsonValueFromContainer(conatiner, "USE_SSL", 7, &val)) {
            delta_options += ", \"ALLOW_HTTP\": \"" + std::string(use_ssl ? "false" : "true") + "\"";
        }

        if (getKeyJsonValueFromContainer(conatiner, "ENDPOINT", 8, &val)) {
            std::string endpoint = GetStringFromJsonbValue(val);
            if (!endpoint.empty() && endpoint.substr(0, 9) == "s3express") {
                delta_options += ", \"AWS_S3_EXPRESS\": \"true\"";
            }
        }
        delta_options += "}";

        SPI_connect();

        Oid argtypes[x_secrets_natts] = {TEXTOID, TEXTOID, TEXTOID, TEXTOID, TEXTOID};
        Datum values[x_secrets_natts] = {CStringGetTextDatum(name.c_str()), CStringGetTextDatum(type.c_str()),
                                         CStringGetTextDatum(scope.c_str()), CStringGetTextDatum(duckdb_query.c_str()),
                                         CStringGetTextDatum(delta_options.c_str())};
        SPIPlanPtr plan = SPI_prepare(
            "INSERT INTO mooncake.secrets (name, type, scope, duckdb_query, delta_options) VALUES ($1, $2, $3, $4, $5)",
            5, argtypes);
        if (plan == NULL) {
            elog(ERROR, "SPI_prepare() failed when creating mooncake secrets.");
        }

        SPI_execute_plan(plan, values, NULL, false, 1);
        SPI_execute("SELECT nextval('mooncake.secrets_table_seq')", false, 1);
        SPI_finish();
    } else {
        elog(ERROR, "Unsupported secret type: %s\n Only secrets of type S3 are supported.", \
                    type.c_str());
    }

    PG_RETURN_VOID();
}
} // extern C