#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdbool.h>

typedef struct {
    char *buffer;
    size_t buffer_length;
    ssize_t input_length;
}InputBuffer;

InputBuffer* new_input_buffer();
void print_prompt();
void read_input(InputBuffer *input_buffer);
void close_input_buffer(InputBuffer *input_buffer);

int main(int agc, char* argv[]){
    InputBuffer *input_buffer = new_input_buffer();
    while (true) {
        print_prompt();
        read_input(input_buffer);

        if (!strcmp(input_buffer->buffer, ".exit")) {
            close_input_buffer(input_buffer);
            exit(EXIT_SUCCESS);
        }else {
            printf("Unrecognized command '%s'.\n", input_buffer->buffer);
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

