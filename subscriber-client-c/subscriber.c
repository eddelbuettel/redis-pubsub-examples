// sample client for pub-sub publisher

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <hiredis/hiredis.h>
#include <hiredis/async.h>
#include <hiredis/adapters/libevent.h>
#include <event2/event.h>

// Structure to hold parsed stock data
typedef struct {
    char fmttime[32];
    double open;
    double high;
    double low;
    double close;
    long volume;
    double percent_change;
    double change;
} stock_data_t;

// Function to parse semicolon-separated stock data string
int parse_stock_data(const char* data_str, stock_data_t* stock_data) {
    if (!data_str || !stock_data) {
        return -1;
    }

    // Create a copy of the string for strtok
    char* data_copy = strdup(data_str);
    if (!data_copy) {
        return -1;
    }

    char* token;
    int field_count = 0;

    // Parse each field separated by semicolon
    token = strtok(data_copy, ";");
    while (token && field_count < 8) {
        switch (field_count) {
            case 0: // formatted time
                memcpy(stock_data->fmttime, token, strlen(token));
                break;
            case 1: // open
                stock_data->open = atof(token);
                break;
            case 2: // high
                stock_data->high = atof(token);
                break;
            case 3: // low
                stock_data->low = atof(token);
                break;
            case 4: // close
                stock_data->close = atof(token);
                break;
            case 5: // volume
                stock_data->volume = atol(token);
                break;
            case 6: // percent_change
                stock_data->percent_change = atof(token);
                break;
            case 7: // change
                stock_data->change = atof(token);
                break;
        }
        field_count++;
        token = strtok(NULL, ";");
    }

    free(data_copy);
    return (field_count == 8) ? 0 : -1;
}

// Function to display parsed stock data
void display_stock_data(const char* symbol, const stock_data_t* data) {
    char buf[32];
    time_t t = time(NULL);
    struct tm *tmp = localtime(&t);
    strftime(buf, sizeof(buf), "%H:%M:%S", tmp);
    printf("%s (%s) %s %.2f %+.2f %+.3f%%\n",
           buf, data->fmttime, symbol,
           data->close, data->change, data->percent_change);
}

// Callback function for subscription messages
void on_message(redisAsyncContext* ac, void* reply, void* privdata) {
    redisReply* r = (redisReply*)reply;

    if (!r) {
        printf("Error: NULL reply received\n");
        return;
    }

    if (r->type == REDIS_REPLY_ARRAY && r->elements >= 3) {
        // Redis pub/sub message format: [message_type, channel, message]
        char* message_type = r->element[0]->str;
        char* channel = r->element[1]->str;
        char* message = r->element[2]->str;

        if (strcmp(message_type, "message") == 0) {
            // Parse the stock data
            stock_data_t stock_data;
            if (parse_stock_data(message, &stock_data) == 0) {
                display_stock_data(channel, &stock_data);
            } else {
                printf("Error: Failed to parse stock data: %s\n", message);
            }
        } else if (strcmp(message_type, "subscribe") == 0) {
            // nothing do -- printf("Successfully subscribed to channel: %s\n", channel);
        }
    } else {
        printf("Unexpected reply format\n");
    }
}

// Callback for connection events
void connect_callback(const redisAsyncContext* ac, int status) {
    if (status != REDIS_OK) {
        printf("Error: Failed to connect to Redis: %s\n", ac->errstr);
        exit(1);
    }
}

// Callback for disconnection events
void disconnect_callback(const redisAsyncContext* ac, int status) {
    if (status != REDIS_OK) {
        printf("Error: Disconnected from Redis: %s\n", ac->errstr);
    } else {
        printf("Disconnected from Redis server\n");
    }
}

// Main subscription function
int subscribe_to_symbol(const char* symbol, const char* redis_host, int redis_port) {
    struct event_base* base = event_base_new();						// Initialize libevent
    if (!base) {
        printf("Error: Could not create event base\n");
        return -1;
    }

    // Create async Redis context
    redisAsyncContext* ac = redisAsyncConnect(redis_host, redis_port);
    if (!ac || ac->err) {
        if (ac) {
            printf("Error: %s\n", ac->errstr);
            redisAsyncFree(ac);
        } else {
            printf("Error: Can't allocate redis context\n");
        }
        event_base_free(base);
        return -1;
    }

    redisLibeventAttach(ac, base);								    // Attach libevent adapter
    redisAsyncSetConnectCallback(ac, connect_callback);				// Set callbacks
    redisAsyncSetDisconnectCallback(ac, disconnect_callback);
    redisAsyncCommand(ac, on_message, NULL, "SUBSCRIBE %s", symbol);// Subscribe to the symbol

    event_base_dispatch(base);										// Start event loop

    redisAsyncFree(ac);												// Cleanup
    event_base_free(base);

    return 0;
}

int main(int argc, char** argv) {
    const char* symbol = "ES1";
    const char* redis_host = "127.0.0.1";
    int redis_port = 6379;

    if (argc > 1) symbol = argv[1];

    // Start subscription
    return subscribe_to_symbol(symbol, redis_host, redis_port);
}

/*
 * Local variables:
 *  mode: c++
 *  compile-command: "gcc -Wall -o subscriber subscriber.c -lhiredis -levent"
 * End:
 */
