#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdbool.h>

typedef struct {
    char *buffer;
    size_t buffer_length;
    ssize_t input_length;
}InputBuffer;

// 元命令识别结果
typedef enum{
    META_SUCCESS,
    META_UNRECOGNIZED_COMMAND,
}MetaResult;

typedef enum {
    PREPARE_SUCCESS,
    PREPARE_UNRECOGNIZED_STATEMENT,
}PreapareResult;

//语句类型
typedef enum {
    INSERT,
    SELECT,
}StatementType;

typedef struct{
    StatementType type;
} Statement;

InputBuffer* new_input_buffer();
void print_prompt();
void read_input(InputBuffer *input_buffer);
void close_input_buffer(InputBuffer *input_buffer);
MetaResult do_mate_command(InputBuffer *input_buffer);
PreapareResult preapare_statement(InputBuffer *input_buffer, Statement *statement);
void execute_statement(Statement statement);

int main(int agc, char* argv[]){
    InputBuffer *input_buffer = new_input_buffer();
    while (true) {
        print_prompt();
        read_input(input_buffer);

        if (input_buffer->buffer[0] == '.') {
            switch (do_mate_command(input_buffer)) {
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
            case PREPARE_UNRECOGNIZED_STATEMENT:
                printf("Unrecognized command '%s'\n", input_buffer->buffer);
                continue;
        }

        execute_statement(statement);
        printf("Executed.\n");
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
do_mate_command(InputBuffer *input_buffer){
    if (!strcmp(input_buffer->buffer, ".exit")) {
        close_input_buffer(input_buffer);
        exit(EXIT_SUCCESS);
    }else {
        return META_UNRECOGNIZED_COMMAND;
    }
}

// 判读语句是否可以执行, 并将可执行的语句类型添加到信息中
PreapareResult
preapare_statement(InputBuffer *input_buffer, Statement *statement){
    if (!strncmp(input_buffer->buffer, "insert", 6)) {
        statement->type = INSERT;
        return PREPARE_SUCCESS;
    }else if (!strcmp(input_buffer->buffer, "select")) {
        statement->type = SELECT;
        return PREPARE_SUCCESS;
    }

    return PREPARE_UNRECOGNIZED_STATEMENT;
}

void
execute_statement(Statement statement){
    switch (statement.type) {
        case INSERT:
            printf("This is a inset statement\n");
            break;
        case SELECT:
            printf("This is a select statement\n");
            break;
    }
}
