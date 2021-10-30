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

MetaCommandResult do_meta_command(InputBuffer *input_buffer) {
    if (strcmp(input_buffer->buffer, ".exit") == 0) {
        return META_COMMAND_SUCCESS;
    } else {
        return META_COMMAND_UNRECOGNIZED_COMMAND;
    }
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
const uint ROWS_PER_PAGE = PAGE_SIZE / ROW_SIZE;
const uint TABLE_MAX_ROWS = ROWS_PER_PAGE * TABLE_MAX_PAGES;

struct Pager {
    int fd;
    uint file_length;
    void *pages[TABLE_MAX_PAGES];
};

struct Table {
    uint num_rows;
    Pager *pager;
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
    uint row_num;
    bool end_of_table;
};

Cursor *table_start(Table *table) {
    auto *cursor = static_cast<Cursor *>(malloc(sizeof(Cursor)));
    cursor->table = table;
    cursor->row_num = 0;
    cursor->end_of_table = (table->num_rows == 0);
    return cursor;
}

Cursor *table_end(Table *table) {
    auto *cursor = static_cast<Cursor *>(malloc(sizeof(Cursor)));
    cursor->table = table;
    cursor->row_num = table->num_rows;
    cursor->end_of_table = true;
    return cursor;
}

void *cursor_value(Cursor *cursor) {
    uint row_num = cursor->row_num;
    uint page_num = row_num / ROWS_PER_PAGE;
    void *page = get_page(cursor->table->pager, page_num);
    uint row_offset = row_num % ROWS_PER_PAGE;
    uint byte_offset = row_offset * ROW_SIZE;
    return (char *) page + byte_offset;
}

void cursor_advance(Cursor *cursor) {
    cursor->row_num += 1;
    if (cursor->row_num >= cursor->table->num_rows) {
        cursor->end_of_table = true;
    }
}

ExecuteResult execute_insert(Statement *statement, Table *table) {
    if (table->num_rows >= TABLE_MAX_ROWS) {
        return EXECUTE_TABLE_FULL;
    }
    Row *row_to_insert = &(statement->row_to_insert);
    auto cursor = table_end(table);

    serialize_row(row_to_insert, cursor_value(cursor));
    table->num_rows += 1;
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

    for (auto &page: pager->pages) {
        page = nullptr;
    }

    return pager;
}

Table *db_open(const char *filename) {
    Pager *pager = pager_open(filename);
    uint num_rows = pager->file_length / ROW_SIZE;
    auto *table = (Table *) malloc(sizeof(Table));
    table->pager = pager;
    table->num_rows = num_rows;
    printf("%s: %d rows data\n", filename, num_rows);
    return table;
}

void pager_flush(Pager *pager, uint page_num, uint size) {
    if (pager->pages[page_num] == nullptr) {
        printf("Tried to flush null page\n");
        exit(EXIT_FAILURE);
    }
    off_t offset = lseek(pager->fd, page_num * PAGE_SIZE, SEEK_SET);
    if (offset == -1) {
        printf("Error seeking\n");
        exit(EXIT_FAILURE);
    }

    ssize_t bytes_written = write(pager->fd, pager->pages[page_num], size);
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
    uint num_full_pages = table->num_rows / PAGE_SIZE;

    for (uint i = 0; i < num_full_pages; i++) {
        if (pager->pages[i] == nullptr) continue;
        pager_flush(pager, i, PAGE_SIZE);
        free(pager->pages[i]);
        pager->pages[i] = nullptr;
    }

    uint num_additional_rows = table->num_rows % ROWS_PER_PAGE;
    if (num_additional_rows > 0) {
        uint page_num = num_full_pages;
        if (pager->pages[page_num] != nullptr) {
            pager_flush(pager, num_full_pages, num_additional_rows * ROW_SIZE);
            free(pager->pages[page_num]);
            pager->pages[page_num] = nullptr;
        }
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
            switch (do_meta_command(input_buffer)) {
                case META_COMMAND_SUCCESS:
                    close_input_buffer(input_buffer);
                    printf("Bye~\n");
                    db_close(table);
                    exit(EXIT_SUCCESS);
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
