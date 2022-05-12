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

typedef enum {
    STATEMENT_INSERT,
    STATEMENT_SELECT,
    STATEMENT_UPDATE,
    STATEMENT_DELETE
}StatementType;

typedef struct {
    StatementType type;
    // data
}Statement;

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

MetaCommandResult do_meta_command(InputBuffer* inputBuffer){
    if(strcmp(inputBuffer->buffer,".exit")==0){
        exit(EXIT_SUCCESS);
    }
    else{
        return META_COMMAND_UNRECOGNIZED_COMMAND;
    }
}

// prepare statement,and output statement
PrepareResult prepare_statement(InputBuffer* inputBuffer,Statement* statement){
    if(strncmp(inputBuffer->buffer,"insert",6)==0){
        statement->type=STATEMENT_INSERT;
        return PREPARE_SUCCESS;
    }
    if(strncmp(inputBuffer->buffer,"select",6)==0){
        statement->type=STATEMENT_INSERT;
        return PREPARE_SUCCESS;
    }


    return PREPARE_UNRECOGNIZED_STATEMENT;
}

void execute_statement(Statement* statement){
    switch (statement->type) {
        case(STATEMENT_INSERT):
            printf("do insert\n");
            break;
        case (STATEMENT_SELECT):
            printf("do select\n");
            break;
        case(STATEMENT_DELETE):
            printf("do delete\n");
            break;
        case(STATEMENT_UPDATE):
            printf("do update\n");
            break;
    }
}






int main(){
    InputBuffer* inputBuffer=newInputBuffer();
    while(1){
        print_prompt();
        read_line(inputBuffer);

//        if(strcmp(inputBuffer->buffer,".exit")==0){
//            close_input_buffer(inputBuffer);
//            exit(EXIT_SUCCESS);//exit(0) 正常退出
//        }
//        else{
//            printf("command not found:%s\n",inputBuffer->buffer);
//        }
        if(inputBuffer->buffer[0]=='.'){
            MetaCommandResult result=do_meta_command(inputBuffer);
            switch (result) {
                case (META_COMMAND_SUCCESS):
                    continue;
                case (META_COMMAND_UNRECOGNIZED_COMMAND):
                    printf("Unrecognized command '%s'\n",inputBuffer->buffer);
                    continue;

            }
        }
        Statement statement;

        PrepareResult prepareResult=prepare_statement(inputBuffer,&statement);
        switch (prepareResult) {
            case PREPARE_SUCCESS:
                break;
            case PREPARE_UNRECOGNIZED_STATEMENT:
                printf("Unrecognized keyword at start of '%s'\n",inputBuffer->buffer);
                continue;
        }

        execute_statement(&statement);
        printf("Executed\n");

    }



}