#include <errno.h>
#include <fcntl.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>


typedef struct{
    char* buffer;
    size_t buffer_length;
    ssize_t input_length;
}InputBuffer;


typedef enum{
    META_COMMAND_SUCCESS,
    META_COMMAND_UNRECOGNIZED_COMMAND
}MetaCommandResult;

typedef enum{
    PREPARE_SUCCESS,
    PREPARE_UNRECOGNIZED_STATEMENT
}PrepareResult;

InputBuffer* newInputBuffer(){
    InputBuffer* inputBuffer=(InputBuffer*)malloc(sizeof(InputBuffer));
    inputBuffer->buffer=NULL;
    inputBuffer->buffer_length=0;
    inputBuffer->input_length=0;

    return inputBuffer;
}

void read_line(InputBuffer* buffer){
    ssize_t read_size=getline(&(buffer->buffer),&(buffer->buffer_length),stdin);

    if(read_size<=0){
        printf("Error reading\n");
        exit(EXIT_FAILURE); //exit(1) 异常退出
    }

    buffer->input_length=read_size-1;//忽略换行符
    buffer->buffer[read_size-1]=0; //忽略换行符

}

void print_prompt() { 
    printf("zjxdb > "); 
}

void close_input_buffer(InputBuffer* buffer){
    free(buffer->buffer);//buffer数组释放
    free(buffer);//InputBuffer本身释放
}



int main(){
    InputBuffer* inputBuffer=newInputBuffer();
    while(1){
        print_prompt();
        read_line(inputBuffer);

        if(strcmp(inputBuffer->buffer,".exit")==0){
            close_input_buffer(inputBuffer);
            exit(EXIT_SUCCESS);//exit(0) 正常退出
        }
        else{
            printf("command not found:%s\n",inputBuffer->buffer);
        }
    }



}