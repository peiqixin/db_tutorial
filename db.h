//
// Created by Wind on 11/4/2021.
//


#ifndef DB_TUTORIAL_DB_H
#define DB_TUTORIAL_DB_H

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <unistd.h>
#include <fcntl.h>

struct InputBuffer {
    char *buffer;
    size_t buffer_length;
};

typedef enum {
    META_COMMAND_SUCCESS,
    META_COMMAND_UNRECOGNIZED_COMMAND,
} MetaCommandResult;

typedef enum {
    PREPARE_SUCCESS,
    PREPARE_NEGATIVE_ID,
    PREPARE_STRING_TOO_LONG,
    PREPARE_UNRECOGNIZED_STATEMENT,
    PREPARE_SYNTAX_ERROR,
} PrepareResult;

typedef enum {
    STATEMENT_INSERT,
    STATEMENT_SELECT,
} StatementType;

#define COLUMN_USERNAME_SIZE 32
#define COLUMN_EMAIL_SIZE 255
struct Row {
    uint id;
    char username[COLUMN_USERNAME_SIZE + 1];
    char email[COLUMN_EMAIL_SIZE + 1];
};

struct Statement {
    StatementType type;
    Row row_to_insert;
};

const uint PAGE_SIZE = 4096;
#define TABLE_MAX_PAGES 100

struct Pager {
    int fd;
    uint file_length;
    uint num_pages;
    void *pages[TABLE_MAX_PAGES];
};

struct Table {
    Pager *pager;
    uint root_page_num;
};


#define size_of_attribute(Struct, Attribute) sizeof(((Struct*)0)->Attribute)

const uint ID_SIZE = size_of_attribute(Row, id);
const uint USERNAME_SIZE = size_of_attribute(Row, username);
const uint EMAIL_SIZE = size_of_attribute(Row, email);
const uint ID_OFFSET = 0;
const uint USERNAME_OFFSET = ID_OFFSET + ID_SIZE;
const uint EMAIL_OFFSET = USERNAME_OFFSET + USERNAME_SIZE;
const uint ROW_SIZE = ID_SIZE + USERNAME_SIZE + EMAIL_SIZE;

enum ExecuteResult {
    EXECUTE_SUCCESS,
    EXECUTE_TABLE_FULL,
    EXECUTE_FAIL,
    EXECUTE_DUPLICATE_KEY,
};

struct Cursor {
    Table *table;
    uint page_num;
    uint cell_num;
    bool end_of_table;
};

typedef enum {
    NODE_INTERNAL,
    NODE_LEAF,
} NodeType;


/**
 * common node header layout
 */
typedef u_int8_t uint8;
const uint NODE_TYPE_SIZE = sizeof(uint8);
const uint NODE_TYPE_OFFSET = 0;
const uint IS_ROOT_SIZE = sizeof(uint8);
const uint IS_ROOT_OFFSET = NODE_TYPE_SIZE;
const uint PARENT_POINTER_SIZE = sizeof(uint);
const uint PARENT_POINTER_OFFSET = IS_ROOT_OFFSET + IS_ROOT_SIZE;
const uint COMMON_NODE_HEADER_SIZE = NODE_TYPE_SIZE + IS_ROOT_SIZE + PARENT_POINTER_SIZE;

/**
 * leaf node header layout
 */
const uint LEAF_NODE_NUM_CELLS_SIZE = sizeof(uint);
const uint LEAF_NODE_NUM_CELLS_OFFSET = COMMON_NODE_HEADER_SIZE;
const uint LEAF_NODE_NEXT_LEAF_SIZE = sizeof(uint);
const uint LEAF_NODE_NEXT_LEAF_OFFSET = LEAF_NODE_NUM_CELLS_SIZE + LEAF_NODE_NUM_CELLS_OFFSET;
const uint LEAF_NODE_HEADER_SIZE = COMMON_NODE_HEADER_SIZE + LEAF_NODE_NUM_CELLS_SIZE + LEAF_NODE_NEXT_LEAF_SIZE;

/**
 * leaf node body layout
 */
const uint LEAF_NODE_KEY_SIZE = sizeof(uint);
const uint LEAF_NODE_KEY_OFFSET = 0;
const uint LEAF_NODE_VALUE_SIZE = ROW_SIZE;
const uint LEAF_NODE_VALUE_OFFSET = LEAF_NODE_KEY_OFFSET + LEAF_NODE_KEY_SIZE;
const uint LEAF_NODE_CELL_SIZE = LEAF_NODE_KEY_SIZE + LEAF_NODE_VALUE_SIZE;
const uint LEAF_NODE_SPACE_FOR_CELLS = PAGE_SIZE - LEAF_NODE_HEADER_SIZE;
const uint LEAF_NODE_MAX_CELLS = LEAF_NODE_SPACE_FOR_CELLS / LEAF_NODE_CELL_SIZE;

const uint LEAF_NODE_RIGHT_SPLIT_COUNT = (LEAF_NODE_MAX_CELLS + 1) / 2;
const uint LEAF_NODE_LEFT_SPLIT_COUNT = (LEAF_NODE_MAX_CELLS + 1) - LEAF_NODE_RIGHT_SPLIT_COUNT;

/**
 * Internal Node Header Layout
 */
const uint INTERNAL_NODE_NUM_KEYS_SIZE = sizeof(uint);
const uint INTERNAL_NODE_NUM_KEYS_OFFSET = COMMON_NODE_HEADER_SIZE;
const uint INTERNAL_NODE_RIGHT_CHILD_SIZE = sizeof(uint);
const uint INTERNAL_NODE_RIGHT_CHILD_OFFSET = INTERNAL_NODE_NUM_KEYS_OFFSET + INTERNAL_NODE_NUM_KEYS_SIZE;
const uint INTERNAL_NODE_HEADER_SIZE =
        COMMON_NODE_HEADER_SIZE + INTERNAL_NODE_NUM_KEYS_SIZE + INTERNAL_NODE_RIGHT_CHILD_SIZE;

/**
 * Internal Node Body Layout
 */
const uint INTERNAL_NODE_KEY_SIZE = sizeof(uint);
const uint INTERNAL_NODE_CHILD_SIZE = sizeof(uint);
const uint INTERNAL_NODE_CELL_SIZE = INTERNAL_NODE_KEY_SIZE + INTERNAL_NODE_CHILD_SIZE;

Table *db_open(const char *filename);

InputBuffer *new_input_buffer();

void print_prompt();

void read_input(InputBuffer *input_buffer);

MetaCommandResult do_meta_command(InputBuffer *input_buffer, Table *table);

PrepareResult prepare_statement(InputBuffer *input_buffer,
                                Statement *statement);

ExecuteResult execute_statement(Statement *statement, Table *table);

void db_close(Table *table);


#endif //DB_TUTORIAL_DB_H