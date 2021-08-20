#include "repl.h"
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>

int main(int agc, char* argv[]){
    if (agc < 2) {
        printf("Must supply a database filename.\n");
        exit(EXIT_FAILURE);
    }

    char *filename = argv[1];
    Table *table = db_open(filename);

    InputBuffer *input_buffer = new_input_buffer();
    while (true) {
        print_prompt();
        read_input(input_buffer);

        if (input_buffer->buffer[0] == '.') {
            switch (do_meta_command(input_buffer, table)) {
                case META_SUCCESS:
                    continue;
                case META_UNRECOGNIZED_COMMAND:
                    printf("Unrecognized command '%s'\n", input_buffer->buffer);
                    continue;
            }
        }

        Statement statement;
        switch (preapare_statement(input_buffer, &statement)) {
            case PREPARE_SUCCESS:
                break;
            case PREPARE_STRING_TOO_LONG:
                printf("String is too long.\n");
                continue;
            case PREPARE_NEGATIVE_ID:
                printf("ID must be positive.\n");
                continue;
            case PREPARE_UNRECOGNIZED_STATEMENT:
                printf("Unrecognized command '%s'\n", input_buffer->buffer);
                continue;
            case PREPARE_SYNTAX_ERROR:
                printf("syntax serror '%s' \n", input_buffer->buffer);
                continue;
        }

        switch (execute_statement(statement, table)) {
            case EXECUTE_SUCCESS:
                printf("Executed. \n");
                break;
            case EXECUTE_DUPLICATE_KEY:
                printf("Error: Duplicate key. \n");
                break;
            case EXECUTE_TABLE_FLL:
                printf("Error: tabe full.\n");
                break;
        }
    }
}

InputBuffer*
new_input_buffer(){
    InputBuffer* input_buffer = malloc(sizeof(InputBuffer));
    input_buffer->buffer = NULL;
    input_buffer->buffer_length = 0;
    input_buffer->input_length = 0;
    return input_buffer;
}

// 打印用户提示符
void
print_prompt(){
    printf("acdb >");
}

// 打印行
void
print_row(Row *row){
    printf("(%d, %s, %s)\n", row->id, row->username, row->email);
}

// 将读取的信息进行一个简单的封装
void
read_input(InputBuffer *input_buffer){
    //getline 的第一个参数会被分配, 所以关闭时要free
    ssize_t bytes_read = getline(&(input_buffer->buffer), &(input_buffer->buffer_length), stdin);
    if (bytes_read <= 0) {
        printf("Error reading input\n");
        exit(EXIT_FAILURE);
    }
    // 忽略结尾的换行符
    input_buffer->input_length = bytes_read - 1;
    input_buffer->buffer[bytes_read - 1] = '\0';
}

// 关闭input_buffer
void
close_input_buffer(InputBuffer *input_buffer){
    free(input_buffer->buffer);
    free(input_buffer);
}

// 识别原名令
MetaResult
do_meta_command(InputBuffer *input_buffer, Table *table){
    if (strcmp(input_buffer->buffer, ".exit") == 0) {
        close_input_buffer(input_buffer);
        db_close(table);
        exit(EXIT_SUCCESS);
    }else if (strcmp(input_buffer->buffer, ".constants") == 0) {
        printf("Constants:\n");
        print_constants();
        return META_SUCCESS;
    }else if (strcmp(input_buffer->buffer, ".btree") == 0) {
        printf("Tree:\n");
        print_leaf_node(get_page(table->pager, 0));
        return META_SUCCESS;
    }else {
        return META_UNRECOGNIZED_COMMAND;
    }
}

// 判读语句是否可以执行, 并将可执行的语句类型添加到信息中
PreapareResult
preapare_statement(InputBuffer *input_buffer, Statement *statement){
    if (!strncmp(input_buffer->buffer, "insert", 6)) {
        return preapare_insert(input_buffer, statement);
    }else if (!strcmp(input_buffer->buffer, "select")) {
        statement->type = SELECT;
        return PREPARE_SUCCESS;
    }

    return PREPARE_UNRECOGNIZED_STATEMENT;
}

PreapareResult
preapare_insert(InputBuffer *input_buffer, Statement *statement){
    statement->type = INSERT;

    char *keyword = strtok(input_buffer->buffer, " ");
    char *id_str = strtok(NULL, " ");
    char *username= strtok(NULL, " ");
    char *email = strtok(NULL, " ");

    // 判断是否都存在
    if (!(id_str && username && email)) {
        return PREPARE_SYNTAX_ERROR;
    }

    int id = atoi(id_str);
    if (id < 0) {
        return PREPARE_NEGATIVE_ID;
    }
    // 长度是否满足
    if (strlen(username) > COLUMN_USERNAME_SIZE || strlen(email) > COLUMN_EMAIL_SIZE) {
        return PREPARE_STRING_TOO_LONG;
    }

    statement->row_to_insert.id = id;
    strcpy(statement->row_to_insert.username, username);
    strcpy(statement->row_to_insert.email, email);
    
    return PREPARE_SUCCESS;
}

ExecuteResult
execute_statement(Statement statement, Table *table){
    switch (statement.type) {
        case INSERT:
            return execute_insert(statement, table);
        case SELECT:
            return execute_select(statement, table);
    }
}

// 向表中插入数据
ExecuteResult
execute_insert(Statement statement, Table *table){
    void *node = get_page(table->pager, table->root_page_num);
    // 当前节点的单元是否已满
    uint32_t num_cells = *leaf_node_num_cells(node);
    if (num_cells >= LEAF_NODE_MAX_CELLS) {
        return EXECUTE_TABLE_FLL;
    }
    Row *row_to_insert = &(statement.row_to_insert);
    uint32_t key_to_insert = row_to_insert->id; 
    Cursor *cursor = table_find(table, key_to_insert);

    if (cursor->cell_num < num_cells) {
        uint32_t key_at_index = *leaf_node_key(node, cursor->cell_num);
        if (key_at_index == key_to_insert) {
            return EXECUTE_DUPLICATE_KEY;
        }
    }

    leaf_node_insert(cursor, row_to_insert->id, row_to_insert);
    free(cursor);
    return EXECUTE_SUCCESS;
}

// 使用游标来读取表，每循环一次游标推进1
ExecuteResult
execute_select(Statement statement, Table *table){
    Cursor* cursor = table_start(table);

    Row row;
    while (!(cursor->end_of_table)) {
        deserialize_row(cursor_value(cursor), &row);
        print_row(&row);
        cursor_advance(cursor);
    }

    free(cursor);
    return EXECUTE_SUCCESS;
}

// 将source信息写入表中的一行
void
serialize_row(Row *source, void *destination) {
    memcpy(destination + ID_OFFSET, &(source->id), ID_SIZE);
    memcpy(destination + USERNAME_OFFSET, &(source->username), USERNAME_SIZE);
    memcpy(destination + EMAIL_OFFSET, &(source->email), EMAIL_SIZE);
}

// 从表中读取信息到 source中
// 此时 source是表的一行, destination存放读取的数据
void
deserialize_row(void *source, Row *destination){
    memcpy(&(destination->id), source + ID_OFFSET, ID_SIZE);
    memcpy(&(destination->username), source + USERNAME_OFFSET, USERNAME_SIZE);
    memcpy(&(destination->email), source + EMAIL_OFFSET, EMAIL_SIZE);
}

// 获取指定数值页的地址，如果该页不再内存中，则加载进内存
void*
get_page(Pager *pager, uint32_t page_num){
    if (page_num >= TABLE_MAX_PAGES ) {
        printf("Tried to fetch page number out of bounds. %d > %d\n", page_num, TABLE_MAX_PAGES);
        exit(EXIT_FAILURE);
    }

    // 缓存中没有, 分配内存，并将同一页的数据加载到内存
    if (pager->pages[page_num] == NULL) {
        void *page = malloc(PAGE_SIZE);
        uint32_t num_pages = pager->file_length / PAGE_SIZE; // 得到现有完整的页数

        // 当存在不满一页的数据时，将其视为一页
        if (pager->file_length % PAGE_SIZE) {
            num_pages++;
        }

        // 如果访问的页中有数据
        if (page_num <= num_pages) {
            lseek(pager->file_descriptor, page_num * PAGE_SIZE, SEEK_SET);
            ssize_t bytes_read = read(pager->file_descriptor, page, PAGE_SIZE);

            if (bytes_read == -1) {
                printf("Error reading file: %d\n", errno);
                exit(EXIT_FAILURE);
            }
        }

        pager->pages[page_num] = page;
        if (page_num >= pager->num_pages) {
            pager->num_pages = page_num + 1;
        }
    }
    return pager->pages[page_num];
}

// 新建表
Table*
db_open(const char *filename){
    Pager *pager = pager_open(filename);

    Table *table = malloc(sizeof(Table));
    table->pager = pager;
    table->root_page_num = 0;

    // 新数据库文件
    if (pager->num_pages == 0) {
        void *root_node = get_page(pager, 0);
        initialize_leaf_node(root_node);
    }

    return table;
}

// 关闭数据库
void
db_close(Table *table){
    Pager *pager = table->pager;

    // 处理填满的页面
    for (uint32_t i = 0; i < pager->num_pages; i++) {
        if (pager->pages[i] == NULL) {
            continue;
        }
        pager_flush(pager, i);
        free(pager->pages[i]);
        pager->pages[i] = NULL;
    }

    int result = close(pager->file_descriptor);
    if (result == -1) {
        printf("Error closing db file.\n");
        exit(EXIT_FAILURE);
    }

    for (uint32_t i = 0; i < TABLE_MAX_PAGES; i++) {
        void *page = pager->pages[i];
        if (page) {
            free(page);
            pager->pages[i] = NULL;
        }
    }

    free(pager);
    free(table);
}

// 将表（内存中）中对应的页些入文件中
void 
pager_flush(Pager *pager, uint32_t page_num){
    if (pager->pages[page_num] == NULL) {
        printf("Tried to flush null page\n");
        exit(EXIT_FAILURE);
    }

    off_t offset = lseek(pager->file_descriptor, page_num * PAGE_SIZE, SEEK_SET);

    if (offset == -1) {
        printf("Error seeking: %d\n", errno);
        exit(EXIT_FAILURE);
    }
    ssize_t bytes_written = write(pager->file_descriptor, pager->pages[page_num], PAGE_SIZE);
    if (bytes_written == -1) {
        printf("Error writing: %d\n", errno);
        exit(EXIT_FAILURE);
    }
}

// 从内存中读取表文件
Pager*
pager_open(const char *filename){
    // 读写模式，不存在创建， 读写权限
    int fd = open(filename, O_RDWR | O_CREAT, S_IWUSR | S_IRUSR );

    if (fd == -1) {
        printf("Unable to open file\n");
        exit(EXIT_FAILURE);
    }

    off_t file_length = lseek(fd, 0, SEEK_END);

    Pager *pager = malloc(sizeof(Pager));
    pager->file_descriptor = fd;
    pager->file_length = file_length;
    pager->num_pages = file_length / PAGE_SIZE;

    // 因为此时每个节点都是一页，所以文件一定是PAGE_SIZE的整数倍
    if (file_length % PAGE_SIZE) {
        printf("Db file is not a whole number of pages. Corrupt file.\n");
        exit(EXIT_FAILURE);
    }

    for (uint32_t i = 0; i < TABLE_MAX_PAGES; i++) {
        pager->pages[i] = NULL;
    }
    return pager;
}

Cursor*
table_start(Table *table){
    Cursor *cursor = malloc(sizeof(Cursor));
    cursor->table = table;
    cursor->page_num = table->root_page_num;
    cursor->cell_num = 0;

    void *root_node = get_page(table->pager, table->root_page_num);
    uint32_t num_cells = *leaf_node_num_cells(root_node);
    cursor->end_of_table = (num_cells == 0);

    return cursor;
}

// 返回给定key的在表中的位置，如果该key存在返回位置，不存在，则返回应该插入的位置
Cursor*
table_find(Table *table, uint32_t key){
    uint32_t root_page_num = table->root_page_num;
    void *root_node = get_page(table->pager, root_page_num);

    if (get_node_type(root_node) == NODE_LEAF) {
        return leaf_node_find(table, root_page_num, key);
    }else {
        printf("Need to implement searching an internal node\n"); 
        exit(EXECUTE_TABLE_FLL);
    }
}

// 推进游标
void
cursor_advance(Cursor *cursor){
    void *node = get_page(cursor->table->pager, cursor->page_num);

    cursor->cell_num++;
    if (cursor->cell_num >= *leaf_node_num_cells(node)) {
        cursor->end_of_table = true;
    }
}

// 获得 游标 在表中指向的地址
void*
cursor_value(Cursor *cursor){
    void *page = get_page(cursor->table->pager, cursor->page_num);

    return leaf_node_value(page, cursor->cell_num);
}
// 获得给定节点的类型
NodeType
get_node_type(void *node){
    return *(uint8_t*)(node + NODE_TYPE_OFFSET);
}

// 设置给定节点的类型
void
set_node_type(void *node, NodeType type){
   *(uint8_t *)(node + NODE_TYPE_OFFSET) = type;
}

// 根据叶节点的首地址，得到该节点的 num_cells 信息
uint32_t*
leaf_node_num_cells(void *node){
    return node + LEAF_NODE_NUM_CELLS_OFFSET;
}

// 得到 node节点的第 cell_num 个cell(键值信息) 的地址
void*
leaf_node_cell(void *node, uint32_t cell_num){
    return node + LEAF_NODE_HEADER_SIZE + LEAF_NODE_CELL_SIZE * cell_num;
}

// 得到 node节点的第 cell_num 个key 的地址
uint32_t*
leaf_node_key(void *node, uint32_t cell_num){
    return leaf_node_cell(node, cell_num);
}

// 得到 node节点的第 cell_num 个value 的地址
uint32_t*
leaf_node_value(void *node, uint32_t cell_num){
    return leaf_node_cell(node, cell_num) + LEAF_NODE_KEY_SIZE;
}

// 初始化一个节点，即将该节点的num_cells值置为0
void
initialize_leaf_node(void *node){
    set_node_type(node, NODE_LEAF); 
    *leaf_node_num_cells(node) = 0;
}

void 
leaf_node_insert(Cursor *cursor, uint32_t key, Row *value){
    void *node = get_page(cursor->table->pager, cursor->page_num);

    // 获取当前要插入的节点的cell数量
    uint32_t num_cells = *leaf_node_num_cells(node);
    if (num_cells >= LEAF_NODE_MAX_CELLS) { // 当前节点空间不够
        printf("Need to implement splitting a leaf node");
        exit(EXIT_FAILURE);
    }

    if (cursor->cell_num < num_cells) {
        for (uint32_t i = num_cells; i > cursor->cell_num; --i) {
            memcpy(leaf_node_cell(node, i), leaf_node_cell(node, i - 1), LEAF_NODE_CELL_SIZE);
        }
    }

    // 插入新节点
    (*leaf_node_num_cells(node))++;
    *leaf_node_key(node, cursor->cell_num) = key;
    serialize_row(value, leaf_node_value(node, cursor->cell_num));
}

Cursor*
leaf_node_find(Table *table, uint32_t page_num, uint32_t key){
    void *node = get_page(table->pager, page_num);
    uint32_t num_cells = *leaf_node_num_cells(node);

    Cursor *cursor = malloc(sizeof(Cursor));
    cursor->page_num = page_num;
    cursor->table = table;

    // 二分查找
    uint32_t left = 0, right = num_cells;
    while (left < right) {
        uint32_t index = (left + right) / 2;
        uint32_t key_at_index = *leaf_node_key(node, index);
        if (key > key_at_index) {
            left = index + 1;
        }else {
            right = index;
        }
    }

    cursor->cell_num = left;
    return cursor;
}

void
print_constants(){
    printf("ROW_SIZE: %d\n", ROW_SIZE);
    printf("COMMON_NODE_HEADER_SIZE: %d\n", COMMON_NODE_HEADER_SIZE);
    printf("LEAF_NODE_HEADER_SIZE: %d\n", LEAF_NODE_HEADER_SIZE);
    printf("LEAF_NODE_CELL_SIZE: %d\n", LEAF_NODE_CELL_SIZE);
    printf("LEAF_NODE_SPACE_FOR_CELLS: %d\n", LEAF_NODE_SPACE_FOR_CELLS);
    printf("LEAF_NODE_MAX_CELLS: %d\n", LEAF_NODE_MAX_CELLS);
}

void
print_leaf_node(void *node){
    uint32_t num_cells = *leaf_node_num_cells(node);
    printf("leaf (size %d)\n", num_cells);
    for (uint32_t i = 0; i < num_cells; i++) {
        uint32_t key = *leaf_node_key(node, i);
        printf("  - %d : %d\n", i, key);
    }
}
