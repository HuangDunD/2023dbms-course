/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#pragma once

#include <cassert>
#include <cstring>
#include <memory>
#include <string>
#include <vector>
#include <ctime>
#include "defs.h"
#include "record/rm_defs.h"
#include "parser/ast.h"

struct TabCol {
    std::string tab_name;
    std::string col_name;
    std::string alias_name;
    ast::SvAggType agg_type;

    friend bool operator<(const TabCol &x, const TabCol &y) {
        return std::make_pair(x.tab_name, x.col_name) < std::make_pair(y.tab_name, y.col_name);
    }

    // 如果table_name和col_name都是相同的, 那么我们就认为这两个Col是相等的
    friend bool operator==(const TabCol &x, const TabCol &y) {
        if(x.tab_name.compare(y.tab_name) || x.col_name.compare(y.col_name))
            return false;
        return true;
    }

};

struct Value {
    ColType type;  // type of value
    union {
        long long bigint_val;      // int value
        int int_val;      // int value
        float float_val;  // float value
    };

    RmRecord raw;  // raw record buffer

    Value() {}

    void setData(ColType type_, size_t col_len, char * buf, size_t buf_len){
        type = type_;
        if (type == TYPE_INT) {
            errno = 0;
            int val = strtoll(buf, NULL, 10);
            if (errno) {
                printf("errON[%d]errMsg[%s]\n",errno,strerror(errno));
                throw ResultOutOfRangeError();
            }
            set_int(val);
        } else if (type == TYPE_BIGINT) {
            errno = 0;
            long long val = strtoll(buf, NULL, 10);
            if (errno) {
                printf("errON[%d]errMsg[%s]\n",errno,strerror(errno));
                throw ResultOutOfRangeError();
            }
            set_bigint(val);
        } else if (type == TYPE_FLOAT) {
            set_float(strtof(buf, NULL));
        } else if (type == TYPE_DATETIME) {
            assert(buf_len == 19 && col_len == 19);
            raw.SetSize(col_len);
            memcpy(raw.data, buf, buf_len);
        } else if (type == TYPE_STRING) {
            assert(buf_len <= col_len);
            raw.SetSize(col_len);
            memcpy(raw.data, buf, buf_len);
        }
    }

    inline std::string getString(){
        return std::string(raw.data, raw.size);
    }

    void check_datetime(std::string str_val_) {
        auto isValidDate = [](int year, int month, int day) -> bool {
            // 检查年份、月份、日期的取值范围
            if ( (year < 1900 || month < 1 || month > 12 || day < 1 || day > 31) || 
                ((month == 4 || month == 6 || month == 9 || month == 11) && day > 30) ) {
                return false;
            }
            if (month == 2) {
                // 检查闰年的情况
                if ((year % 4 == 0 && year % 100 != 0) || (year % 400 == 0)) {
                    if (day > 29) {
                        return false;
                    }
                } else {
                    if (day > 28) {
                        return false;
                    }
                }
            }
            return true;
        };

        tm timeinfo;
        timeinfo.tm_isdst=-1;

        int readnums = sscanf(str_val_.c_str(), "%d-%d-%d %d:%d:%d", &timeinfo.tm_year, &timeinfo.tm_mon, &timeinfo.tm_mday, &timeinfo.tm_hour, &timeinfo.tm_min, &timeinfo.tm_sec);

        bool match_res;
        if (str_val_.size() != 19) match_res = false;
        else {
            char s[20] = {0};
            for (int i = 0; i < 19; i++)
                s[i] = ('0' <= str_val_[i] && str_val_[i] <= '9') ? '0' : str_val_[i];
            match_res = strcmp(s, "0000-00-00 00:00:00") == 0;
        }

        if (!match_res || !isValidDate(timeinfo.tm_year, timeinfo.tm_mon, timeinfo.tm_mday) || readnums != 6 || timeinfo.tm_hour < 0 || timeinfo.tm_min < 0 || timeinfo.tm_sec < 0 || timeinfo.tm_hour > 23 || timeinfo.tm_min > 59 || timeinfo.tm_sec > 59) {
            throw DateTimeFormatError();
        }
    }

    void set_int(int int_val_) {
        type = TYPE_INT;
        int_val = int_val_;
        raw.SetSize(sizeof(int));
        *(int *)(raw.data) = int_val;
    }

    void set_bigint(long long bigint_val_) {
        type = TYPE_BIGINT;
        bigint_val = bigint_val_;
        raw.SetSize(sizeof(long long));
        *(long long *)(raw.data) = bigint_val;
    }

    void set_float(float float_val_) {
        type = TYPE_FLOAT;
        float_val = float_val_;
        raw.SetSize(sizeof(float));
        *(float *)(raw.data) = float_val;
    }

    void set_str(std::string& str_val_) {
        type = TYPE_STRING;
        raw.SetSize(str_val_.size());
        memcpy(raw.data, str_val_.c_str(), str_val_.size());
    }

    void convert_value_to_col_type_and_fill(const ColMeta &col) {
        if (col.type == TYPE_INT && type == TYPE_BIGINT) {
            set_int(bigint_val);
        } else if (col.type == TYPE_BIGINT && type == TYPE_INT) {
            set_bigint(int_val);
        } else if (col.type == TYPE_FLOAT && type == TYPE_INT) {
            set_float(int_val);
        } else if (col.type == TYPE_FLOAT && type == TYPE_BIGINT) {
            set_float(bigint_val);
        } else if (col.type == TYPE_DATETIME && type == TYPE_STRING) {
            check_datetime(getString());
            type = TYPE_DATETIME;
        } else if (col.type == TYPE_STRING){
            if (raw.size > col.len)
                throw StringOverflowError();
            if (raw.size < col.len) {
                char* tmp = raw.data;
                size_t tmp_len = raw.size;
                raw.data = new char[col.len];
                raw.size = col.len;
                memset(raw.data, 0, col.len);
                memcpy(raw.data, tmp, tmp_len);
                delete[] tmp;
            }
        }
    }

};

enum CompOp { OP_EQ, OP_NE, OP_LT, OP_GT, OP_LE, OP_GE };

struct Condition {
    TabCol lhs_col;   // left-hand side column
    CompOp op;        // comparison operator
    bool is_rhs_val;  // true if right-hand side is a value (not a column)
    TabCol rhs_col;   // right-hand side column
    Value rhs_val;    // right-hand side value
};

struct SetClause {
    TabCol lhs;
    Value rhs;
    TabCol rhs_col;
};