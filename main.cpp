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

#define size_of_attribute(Struct, Attribute) sizeof(((Struct*)0)->Attribute)

const uint ID_SIZE = size_of_attribute(Row, id);
const uint USERNAME_SIZE = size_of_attribute(Row, username);
const uint EMAIL_SIZE = size_of_attribute(Row, email);
const uint ID_OFFSET = 0;
const uint USERNAME_OFFSET = ID_OFFSET + ID_SIZE;
const uint EMAIL_OFFSET = USERNAME_OFFSET + USERNAME_SIZE;
const uint ROW_SIZE = ID_SIZE + USERNAME_SIZE + EMAIL_SIZE;

InputBuffer *new_input_buffer() {
    auto *input_buffer = (InputBuffer *) malloc(sizeof(InputBuffer));
    input_buffer->buffer = nullptr;
    input_buffer->buffer_length = 0;
    return input_buffer;
}

void print_prompt() {
    printf("db > ");
}

void read_input(InputBuffer *input_buffer) {
    ssize_t bytes_read = getline(&(input_buffer->buffer), &(input_buffer->buffer_length), stdin);
    if (bytes_read <= 0) {
        printf("Error reading input\n");
        exit(EXIT_FAILURE);
    }
    input_buffer->buffer[bytes_read - 1] = 0;
}

void close_input_buffer(InputBuffer *input_buffer) {
    free(input_buffer->buffer);
    free(input_buffer);
}


PrepareResult prepare_insert(InputBuffer *input_buffer, Statement *statement) {
    statement->type = STATEMENT_INSERT;
    char *keyword = strtok(input_buffer->buffer, " ");
    char *id_string = strtok(nullptr, " ");
    char *username = strtok(nullptr, " ");
    char *email = strtok(nullptr, " ");
    if (!keyword || !id_string || !username || !email) {
        return PREPARE_SYNTAX_ERROR;
    }
    int id = atoi(id_string);
    if (id < 0) {
        return PREPARE_NEGATIVE_ID;
    }
    if (strlen(username) > COLUMN_USERNAME_SIZE) {
        return PREPARE_STRING_TOO_LONG;
    }
    if (strlen(email) > COLUMN_EMAIL_SIZE) {
        return PREPARE_STRING_TOO_LONG;
    }

    statement->row_to_insert.id = id;
    strcpy(statement->row_to_insert.username, username);
    strcpy(statement->row_to_insert.email, email);
    return PREPARE_SUCCESS;
}

PrepareResult prepare_statement(InputBuffer *input_buffer,
                                Statement *statement) {
    if (strncmp(input_buffer->buffer, "insert", 6) == 0) {
        return prepare_insert(input_buffer, statement);
    }
    if (strcmp(input_buffer->buffer, "select") == 0) {
        statement->type = STATEMENT_SELECT;
        return PREPARE_SUCCESS;
    }
    return PREPARE_UNRECOGNIZED_STATEMENT;
}

void serialize_row(Row *source, void *destination) {
    char *dest = (char *) destination;
    memcpy(dest + ID_OFFSET, &(source->id), ID_SIZE);
    strncpy(dest + USERNAME_OFFSET, source->username, USERNAME_SIZE);
    strncpy(dest + EMAIL_OFFSET, source->email, EMAIL_SIZE);
}

void deserialize_row(void *source, Row *dest) {
    char *src = (char *) source;
    memcpy(&(dest->id), src + ID_OFFSET, ID_SIZE);
    memcpy(dest->username, src + USERNAME_OFFSET, USERNAME_SIZE);
    memcpy(dest->email, src + EMAIL_OFFSET, EMAIL_SIZE);
}

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

void print(Row *row) {
    printf("(%d %s %s)\n", row->id, row->username, row->email);
}

void *get_page(Pager *pager, uint page_num) {
    if (page_num > TABLE_MAX_PAGES) {
        printf("Tried to fetch page number out of bounds. %d > %d\n", page_num,
               TABLE_MAX_PAGES);
        exit(EXIT_FAILURE);
    }
    if (pager->pages[page_num] == nullptr) {
        // malloc one page
        auto page = malloc(PAGE_SIZE);
        uint num_pages = pager->file_length / PAGE_SIZE;
        if (pager->file_length % PAGE_SIZE) {
            num_pages += 1;
        }
        // TODO
        if (page_num <= num_pages) {
            lseek(pager->fd, page_num * PAGE_SIZE, SEEK_SET);
            ssize_t bytes_read = read(pager->fd, page, PAGE_SIZE);
            if (bytes_read == -1) {
                printf("Error reading file\n");
                exit(EXIT_FAILURE);
            }
        }
        pager->pages[page_num] = page;

        if (pager->num_pages <= page_num) {
            pager->num_pages = page_num + 1;
        }
    }
    return pager->pages[page_num];
}

enum ExecuteResult {
    EXECUTE_SUCCESS,
    EXECUTE_TABLE_FULL,
    EXECUTE_FAIL,
};

struct Cursor {
    Table *table;
    uint page_num;
    uint cell_num;
    bool end_of_table;
};

Pager *pager_open(const char *filename) {
    int fd = open(filename, O_RDWR | O_CREAT, S_IWUSR | S_IRUSR);
    if (fd == -1) {
        printf("Unable to open file\n");
        exit(EXIT_FAILURE);
    }

    off_t file_length = lseek(fd, 0, SEEK_END);

    auto *pager = (Pager *) malloc(sizeof(Pager));
    pager->fd = fd;
    pager->file_length = file_length;
    pager->num_pages = (file_length / PAGE_SIZE);

    if (file_length % PAGE_SIZE) {
        printf("DB file is not a whole number of pages. Corrupt file\n");
        exit(EXIT_FAILURE);
    }

    for (auto &page: pager->pages) {
        page = nullptr;
    }

    return pager;
}


void pager_flush(Pager *pager, uint page_num) {
    if (pager->pages[page_num] == nullptr) {
        printf("Tried to flush null page\n");
        exit(EXIT_FAILURE);
    }
    off_t offset = lseek(pager->fd, page_num * PAGE_SIZE, SEEK_SET);
    if (offset == -1) {
        printf("Error seeking\n");
        exit(EXIT_FAILURE);
    }

    ssize_t bytes_written = write(pager->fd, pager->pages[page_num], PAGE_SIZE);
    if (bytes_written == -1) {
        printf("Error writing\n");
        exit(EXIT_FAILURE);
    }
}

/**
 * 1. flushes the page cache to disk
 * 2. closes the database file
 * 3. frees the memory for the pager and table data structures
 * @param table
 */
void db_close(Table *table) {
    Pager *pager = table->pager;

    for (uint i = 0; i < pager->num_pages; i++) {
        if (pager->pages[i] == nullptr) continue;
        pager_flush(pager, i);
        free(pager->pages[i]);
        pager->pages[i] = nullptr;
    }

    int result = close(pager->fd);
    if (result == -1) {
        printf("Error closing db file\n");
        exit(EXIT_FAILURE);
    }

    for (auto &page: pager->pages) {
        if (page) {
            free(page);
            page = nullptr;
        }
    }

    free(pager);
    free(table);
}

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
const uint LEAF_NODE_HEADER_SIZE = COMMON_NODE_HEADER_SIZE + LEAF_NODE_NUM_CELLS_SIZE;

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

uint *leaf_node_num_cells(void *node) {
    return reinterpret_cast<uint *>((char *) node + LEAF_NODE_NUM_CELLS_OFFSET);
}

void *leaf_node_cell(void *node, uint cell_num) {
    return (char *) node + LEAF_NODE_HEADER_SIZE + cell_num * LEAF_NODE_CELL_SIZE;
}

uint *leaf_node_key(void *node, uint cell_num) {
    return static_cast<uint *>(leaf_node_cell(node, cell_num));
}

void *leaf_node_value(void *node, uint cell_num) {
    return (char *) leaf_node_cell(node, cell_num) + LEAF_NODE_KEY_SIZE;
}

void initialize_leaf_node(void *node) {
    *leaf_node_num_cells(node) = 0;
}

Cursor *table_start(Table *table) {
    auto *cursor = static_cast<Cursor *>(malloc(sizeof(Cursor)));
    cursor->table = table;
    cursor->page_num = table->root_page_num;
    cursor->cell_num = 0;

    void *root_node = get_page(table->pager, table->root_page_num);
    uint num_cells = *leaf_node_num_cells(root_node);
    cursor->end_of_table = (num_cells == 0);

    return cursor;
}

Cursor *table_end(Table *table) {
    auto *cursor = static_cast<Cursor *>(malloc(sizeof(Cursor)));
    cursor->table = table;
    cursor->page_num = table->root_page_num;

    void *root_node = get_page(table->pager, table->root_page_num);
    uint num_cells = *leaf_node_num_cells(root_node);
    cursor->cell_num = num_cells;
    cursor->end_of_table = true;

    return cursor;
}

void *cursor_value(Cursor *cursor) {
    uint page_num = cursor->page_num;
    void *page = get_page(cursor->table->pager, page_num);
    return leaf_node_value(page, cursor->cell_num);
}

void cursor_advance(Cursor *cursor) {
    uint page_num = cursor->page_num;
    void *node = get_page(cursor->table->pager, page_num);
    cursor->cell_num += 1;

    if (cursor->cell_num >= *leaf_node_num_cells(node)) {
        cursor->end_of_table = true;
    }
}

void leaf_node_insert(Cursor *cursor, uint key, Row *value) {
    void *node = get_page(cursor->table->pager, cursor->page_num);
    uint num_cells = *leaf_node_num_cells(node);
    if (num_cells >= LEAF_NODE_MAX_CELLS) {
        printf("Need to implement splitting a leaf node\n");
        exit(EXIT_FAILURE);
    }

    if (cursor->cell_num < num_cells) {
        for (uint i = num_cells; i > cursor->cell_num; --i) {
            memcpy(leaf_node_cell(node, i), leaf_node_cell(node, i - 1), LEAF_NODE_CELL_SIZE);
        }
    }

    *leaf_node_num_cells(node) += 1;
    *(leaf_node_key(node, cursor->cell_num)) = key;
    serialize_row(value, leaf_node_value(node, cursor->cell_num));
}


Table *db_open(const char *filename) {
    Pager *pager = pager_open(filename);
    auto *table = (Table *) malloc(sizeof(Table));
    table->pager = pager;
    table->root_page_num = 0;
    if (pager->num_pages == 0) {
        // new data file
        void *root_node = get_page(pager, 0);
        initialize_leaf_node(root_node);
    }
    return table;
}

ExecuteResult execute_insert(Statement *statement, Table *table) {
    void *node = get_page(table->pager, table->root_page_num);
    if (*leaf_node_num_cells(node) > LEAF_NODE_MAX_CELLS) {
        return EXECUTE_TABLE_FULL;
    }
    Row *row_to_insert = &(statement->row_to_insert);
    auto cursor = table_end(table);
    leaf_node_insert(cursor, row_to_insert->id, row_to_insert);
    return EXECUTE_SUCCESS;
}

ExecuteResult execute_select(Table *table) {
    auto cursor = table_start(table);
    Row row{};
    while (!cursor->end_of_table) {
        deserialize_row(cursor_value(cursor), &row);
        print(&row);
        cursor_advance(cursor);
    }
    return EXECUTE_SUCCESS;
}

ExecuteResult execute_statement(Statement *statement, Table *table) {
    switch (statement->type) {
        case (STATEMENT_INSERT):
            return execute_insert(statement, table);
        case (STATEMENT_SELECT):
            return execute_select(table);
    }
    return EXECUTE_FAIL;
}

void print_constants() {
    printf("ROW_SIZE: %d\n", ROW_SIZE);
    printf("COMMON_NODE_HEADER_SIZE: %d\n", COMMON_NODE_HEADER_SIZE);
    printf("LEAF_NODE_HEADER_SIZE: %d\n", LEAF_NODE_HEADER_SIZE);
    printf("LEAF_NODE_CELL_SIZE: %d\n", LEAF_NODE_CELL_SIZE);
    printf("LEAF_NODE_SPACE_FOR_CELLS: %d\n", LEAF_NODE_SPACE_FOR_CELLS);
    printf("LEAF_NODE_MAX_CELLS: %d\n", LEAF_NODE_MAX_CELLS);
}

void print_leaf_node(void *node) {
    uint num_cells = *leaf_node_num_cells(node);
    printf("leaf (size %d)\n", num_cells);
    for (uint i = 0; i < num_cells; i++) {
        uint key = *leaf_node_key(node, i);
        printf("  - %d : %d\n", i, key);
    }
}

MetaCommandResult do_meta_command(InputBuffer *input_buffer, Table *table) {
    if (strcmp(input_buffer->buffer, ".exit") == 0) {
        close_input_buffer(input_buffer);
        printf("Bye~\n");
        db_close(table);
        exit(EXIT_SUCCESS);
    } else if (strcmp(input_buffer->buffer, ".constants") == 0) {
        printf("Constants:\n");
        print_constants();
        return META_COMMAND_SUCCESS;
    } else if (strcmp(input_buffer->buffer, ".btree") == 0) {
        printf("Tree:\n");
        print_leaf_node(get_page(table->pager, 0));
        return META_COMMAND_SUCCESS;
    } else {
        return META_COMMAND_UNRECOGNIZED_COMMAND;
    }
}


int main(int argc, const char *argv[]) {
    if (argc < 2) {
        printf("Must supply a database filename\n");
        return 0;
    }
    const char *filename = argv[1];
    Table *table = db_open(filename);
    InputBuffer *input_buffer = new_input_buffer();
    while (true) {
        print_prompt();
        read_input(input_buffer);
        if (input_buffer->buffer[0] == '.') {
            switch (do_meta_command(input_buffer, table)) {
                case META_COMMAND_SUCCESS:
                    continue;
                case META_COMMAND_UNRECOGNIZED_COMMAND:
                    printf("Unrecognized command %s\n", input_buffer->buffer);
                    continue;
            }
        }
        Statement statement{};
        switch (prepare_statement(input_buffer, &statement)) {
            case PREPARE_SUCCESS:
                break;
            case PREPARE_UNRECOGNIZED_STATEMENT:
                printf("Unrecognized keyword at start of %s\n", input_buffer->buffer);
                break;
            case PREPARE_SYNTAX_ERROR:
                printf("Syntax error. Could not parse statement\n");
                break;
            case PREPARE_STRING_TOO_LONG:
                printf("String is too long\n");
                break;
            case PREPARE_NEGATIVE_ID:
                printf("ID must be positive\n");
                break;
        }
        switch (execute_statement(&statement, table)) {
            case (EXECUTE_SUCCESS):
                printf("Executed\n");
                break;
            case (EXECUTE_TABLE_FULL):
                printf("Error: Table full\n");
                break;
            case EXECUTE_FAIL:
                break;
        }
    }
}
