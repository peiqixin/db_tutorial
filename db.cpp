//
// Created by Wind on 11/4/2021.
//

#include "db.h"

uint *leaf_node_next_leaf(void *node);

Cursor *table_start(Table *table);


void print_prompt() {
    printf("db > ");
}


InputBuffer *new_input_buffer() {
    auto *input_buffer = (InputBuffer *) malloc(sizeof(InputBuffer));
    input_buffer->buffer = nullptr;
    input_buffer->buffer_length = 0;
    return input_buffer;
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


PrepareResult prepare_statement(InputBuffer *input_buffer, Statement *statement) {
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
        uint next_page_num = *leaf_node_next_leaf(node);
        if (next_page_num == 0) {
            cursor->end_of_table = true;
        } else {
            cursor->page_num = next_page_num;
            cursor->cell_num = 0;
        }
    }
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


void print_constants() {
    printf("ROW_SIZE: %d\n", ROW_SIZE);
    printf("COMMON_NODE_HEADER_SIZE: %d\n", COMMON_NODE_HEADER_SIZE);
    printf("LEAF_NODE_HEADER_SIZE: %d\n", LEAF_NODE_HEADER_SIZE);
    printf("LEAF_NODE_CELL_SIZE: %d\n", LEAF_NODE_CELL_SIZE);
    printf("LEAF_NODE_SPACE_FOR_CELLS: %d\n", LEAF_NODE_SPACE_FOR_CELLS);
    printf("LEAF_NODE_MAX_CELLS: %d\n", LEAF_NODE_MAX_CELLS);
}


Cursor *leaf_node_find(Table *table, uint page_num, uint key) {
    void *node = get_page(table->pager, page_num);
    uint num_cells = *leaf_node_num_cells(node);

    auto *cursor = static_cast<Cursor *>(malloc(sizeof(Cursor)));
    cursor->table = table;
    cursor->page_num = page_num;

    uint min_index = 0;
    uint one_past_max_index = num_cells;
    while (min_index != one_past_max_index) {
        uint index = (min_index + one_past_max_index) / 2;
        uint key_at_index = *leaf_node_key(node, index);
        if (key == key_at_index) {
            cursor->cell_num = index;
            return cursor;
        }
        if (key < key_at_index) {
            one_past_max_index = index;
        } else {
            min_index = index + 1;
        }
    }
    cursor->cell_num = min_index;
    return cursor;
}


NodeType get_node_type(void *node) {
    uint8 value = *((uint *) ((char *) node + NODE_TYPE_OFFSET));
    return static_cast<NodeType>(value);
}


void set_node_type(void *node, NodeType type) {
    uint value = type;
    *((uint *) ((char *) node + NODE_TYPE_OFFSET)) = value;
}


uint *internal_node_num_keys(void *node) {
    return reinterpret_cast<uint *>((char *) node + INTERNAL_NODE_NUM_KEYS_OFFSET);
}


uint *internal_node_right_child(void *node) {
    return reinterpret_cast<uint *>((char *) node + INTERNAL_NODE_RIGHT_CHILD_OFFSET);
}


uint *internal_node_cell(void *node, uint cell_num) {
    return reinterpret_cast<uint *>((char *) node + INTERNAL_NODE_HEADER_SIZE + cell_num * INTERNAL_NODE_CELL_SIZE);
}


uint *internal_node_child(void *node, uint child_num) {
    uint num_keys = *internal_node_num_keys(node);
    if (child_num > num_keys) {
        printf("Tried to access child_num %d > num_keys %d\n", child_num, num_keys);
        exit(EXIT_FAILURE);
    } else if (child_num == num_keys) {
        return internal_node_right_child(node);
    } else {
        return internal_node_cell(node, child_num);
    }
}


uint *internal_node_key(void *node, uint key_num) {
    return internal_node_cell(node, key_num) + INTERNAL_NODE_CHILD_SIZE;
}


uint get_node_max_key(void *node) {
    switch (get_node_type(node)) {
        case NODE_INTERNAL:
            return *internal_node_key(node, *internal_node_num_keys(node));
        case NODE_LEAF:
            return *leaf_node_key(node, *leaf_node_num_cells(node) - 1);
        default:
            printf("Unknown node type\n");
            exit(EXIT_FAILURE);
    }
}


bool is_node_root(void *node) {
    uint8 value = *((uint8 *) ((char *) node + IS_ROOT_OFFSET));
    return value;
}


void set_node_root(void *node, bool is_root) {
    uint8 value = is_root;
    *((uint8 *) ((char *) node + IS_ROOT_OFFSET)) = value;
}


uint get_unused_page_num(Pager *pager) {
    return pager->num_pages;
}


void initialize_leaf_node(void *node) {
    set_node_type(node, NODE_LEAF);
    set_node_root(node, false);
    *leaf_node_num_cells(node) = 0;
    *leaf_node_next_leaf(node) = 0;
}


void initialize_internal_node(void *node) {
    set_node_type(node, NODE_INTERNAL);
    set_node_root(node, false);
    *internal_node_num_keys(node) = 0;
}


void create_new_root(Table *table, uint right_child_page_num) {
    void *root = get_page(table->pager, table->root_page_num);
//    void *right_child = get_page(table->pager, right_child_page_num);
    uint left_child_page_num = get_unused_page_num(table->pager);
    void *left_child = get_page(table->pager, left_child_page_num);

    /* Left child has data copied from old root */
    memcpy(left_child, root, PAGE_SIZE);
    set_node_root(left_child, false);

    /* Root node is a new internal node with one key and two children */
    initialize_internal_node(root);
    set_node_root(root, true);
    *internal_node_num_keys(root) = 1;

    *internal_node_child(root, 0) = left_child_page_num;
    uint left_child_max_key = get_node_max_key(left_child);
    *internal_node_key(root, 0) = left_child_max_key;
    *internal_node_right_child(root) = right_child_page_num;
}


void leaf_node_split_and_insert(Cursor *cursor, uint key, Row *value) {
    void *old_node = get_page(cursor->table->pager, cursor->page_num);
    uint new_page_num = get_unused_page_num(cursor->table->pager);
    void *new_node = get_page(cursor->table->pager, new_page_num);
    initialize_leaf_node(new_node);
    *leaf_node_next_leaf(new_node) = *leaf_node_next_leaf(old_node);
    *leaf_node_next_leaf(old_node) = new_page_num;
    for (int i = LEAF_NODE_MAX_CELLS; i >= 0; i--) {
        void *destination_node;
        if (i >= LEAF_NODE_LEFT_SPLIT_COUNT) {
            destination_node = new_node;
        } else {
            destination_node = old_node;
        }
        uint index_within_node = i % LEAF_NODE_LEFT_SPLIT_COUNT;
        void *destination = leaf_node_cell(destination_node, index_within_node);

        if (i == cursor->cell_num) {
            serialize_row(value, leaf_node_value(destination_node, index_within_node));
            *leaf_node_key(destination_node, index_within_node) = key;
        } else if (i > cursor->cell_num) {
            memcpy(destination, leaf_node_cell(old_node, i - 1), LEAF_NODE_CELL_SIZE);
        } else {
            memcpy(destination, leaf_node_cell(old_node, i), LEAF_NODE_CELL_SIZE);
        }
    }

    *(leaf_node_num_cells(old_node)) = LEAF_NODE_LEFT_SPLIT_COUNT;
    *(leaf_node_num_cells(new_node)) = LEAF_NODE_RIGHT_SPLIT_COUNT;

    if (is_node_root(old_node)) {
        create_new_root(cursor->table, new_page_num);
    } else {
        printf("Need to implement updating parent after split\n");
        exit(EXIT_FAILURE);
    }
}


void leaf_node_insert(Cursor *cursor, uint key, Row *value) {
    void *node = get_page(cursor->table->pager, cursor->page_num);
    uint num_cells = *leaf_node_num_cells(node);
    if (num_cells >= LEAF_NODE_MAX_CELLS) {
        leaf_node_split_and_insert(cursor, key, value);
        return;
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


void indent(uint level) {
    for (uint i = 0; i < level; ++i) printf("  ");
}


void print_tree(Pager *pager, uint page_num, uint level) {
    void *node = get_page(pager, page_num);
    uint num_keys, child;

    switch (get_node_type(node)) {
        case NODE_LEAF:
            num_keys = *leaf_node_num_cells(node);
            indent(level);
            printf("- leaf (size %d)\n", num_keys);
            for (uint i = 0; i < num_keys; i++) {
                indent(level + 1);
                printf("- %d\n", *leaf_node_key(node, i));
            }
            break;
        case NODE_INTERNAL:
            num_keys = *internal_node_num_keys(node);
            indent(level);
            printf("- internal (size %d)\n", num_keys);
            for (uint i = 0; i < num_keys; i++) {
                child = *internal_node_child(node, i);
                print_tree(pager, child, level + 1);

                indent(level);
                printf("- key %d\n", *internal_node_key(node, i));
            }
            child = *internal_node_right_child(node);
            print_tree(pager, child, level + 1);
            break;
    }
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
        set_node_root(root_node, true);
    }
    return table;
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
        print_tree(table->pager, 0, 0);
        return META_COMMAND_SUCCESS;
    } else {
        return META_COMMAND_UNRECOGNIZED_COMMAND;
    }
}


Cursor *internal_node_find(Table *table, uint page_num, uint key) {
    void *node = get_page(table->pager, page_num);
    uint num_keys = *internal_node_num_keys(node);
    uint min_index = 0;
    uint max_index = num_keys;

    while (min_index != max_index) {
        uint index = (min_index + max_index) / 2;
        uint key_to_right = *internal_node_key(node, index);
        if (key_to_right >= key) {
            max_index = index;
        } else {
            min_index = index + 1;
        }
    }

    uint child_num = *internal_node_child(node, min_index);
    void *child = get_page(table->pager, child_num);
    switch (get_node_type(child)) {
        case NODE_LEAF:
            return leaf_node_find(table, child_num, key);
        case NODE_INTERNAL:
            return internal_node_find(table, child_num, key);
        default:
            printf("Unknown node type\n");
            exit(EXIT_FAILURE);
    }
}


Cursor *table_find(Table *table, uint key) {
    uint root_page_num = table->root_page_num;
    void *root_node = get_page(table->pager, root_page_num);

    if (get_node_type(root_node) == NODE_LEAF) {
        return leaf_node_find(table, root_page_num, key);
    } else {
        return internal_node_find(table, root_page_num, key);
    }
}


ExecuteResult execute_insert(Statement *statement, Table *table) {
    void *node = get_page(table->pager, table->root_page_num);
    uint num_cells = *leaf_node_num_cells(node);

    Row *row_to_insert = &(statement->row_to_insert);
    uint key_to_insert = row_to_insert->id;
    Cursor *cursor = table_find(table, key_to_insert);

    if (cursor->cell_num < num_cells) {
        uint key_at_index = *leaf_node_key(node, cursor->cell_num);
        if (key_at_index == key_to_insert) {
            return EXECUTE_DUPLICATE_KEY;
        }
    }

    leaf_node_insert(cursor, row_to_insert->id, row_to_insert);
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


Cursor *table_start(Table *table) {
    auto *cursor = table_find(table, 0);
    void *node = get_page(table->pager, cursor->page_num);
    uint num_cells = *leaf_node_num_cells(node);
    cursor->end_of_table = (num_cells == 0);

    return cursor;
}


uint *leaf_node_next_leaf(void *node) {
    return reinterpret_cast<uint *>((char *) node + LEAF_NODE_NEXT_LEAF_OFFSET);
}
