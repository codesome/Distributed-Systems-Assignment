/*
**   Author: Ganesh Vernekar (CS15BTECH11018)
**/

#ifndef __COMMON_H__
#define __COMMON_H__

const int max_size = 65536;

enum COLOR : char {
    WHITE = 0, RED = 1
};

// Packet types.
enum MESSAGE_TYPE : char {
    DATA = 0, CONTROL = 1, TERMINATE = 2
};

// Control Packet subtypes.
enum CONTROL_TYPE : char {
    MARKER = 0, SNAPSHOT = 1
};

// Functions to read and write int and char to void* buffer.

void increment(void **ptr, int offset) {
    *ptr = ((char*)(*ptr)) + offset;
}

void* write_char(void *ptr, char val) {
    *((char*)ptr) = val;
    return (char*)ptr + sizeof(char);
}

void* write_int(void *ptr, int val) {
    *((int*)ptr) = val;
    return (char*)ptr + sizeof(int);
}

char read_char(void **ptr) {
    char v = *((char*)(*ptr));
    *ptr = (char*)(*ptr) + sizeof(char);
    return v;
}

int read_int(void **ptr) {
    int v = *((int*)(*ptr));
    *ptr = (char*)(*ptr) + sizeof(int);
    return v;
}

const int message_marshal_size = (2*sizeof(int)) + (3*sizeof(char));
// Message sent for transation.
struct message {
    int id;
    char from, to;
    COLOR color;
    int amount;

    message() {}

    message(int id, char from, char to, COLOR color) {
        this->id = id;
        this->from = from;
        this->to = to;
        this->color = color;
    }

    // Encode message into storable format.
    int marshal(void *original) {
        void *ptr = original;
        ptr = write_int(ptr, id);
        ptr = write_char(ptr, from);
        ptr = write_char(ptr, to);
        ptr = write_char(ptr, color);
        ptr = write_int(ptr, amount);
        return message_marshal_size;
    }
    
    // Decode message from bytes.
    void unmarshal(void *original) {
        void *ptr = original;
        id = read_int(&ptr);
        from = read_char(&ptr);
        to = read_char(&ptr);
        color = (COLOR)read_char(&ptr);
        amount = read_int(&ptr);
    }
};

// Used to pass packets from receive thread to processing thread.
struct buffer_message {
    void *data;
    int size;
    buffer_message(): data(NULL), size(0) {}
    buffer_message(void *data, int size): data(data), size(size) {}
};

#endif