#!/usr/bin/env r

library(RcppRedis)

## Callback handler for convenience
symbolRedisMonitorChannel <- function(context, type="string") {
    res <- context$listen(type)
    if (length(res) != 3 || res[[1]] != "message")
        return(res)

    ## here we do a simple text parse of the ';' separated token
    data <- read.csv(text=res[[3]], sep=";", header=FALSE,
                     col.names = c("Time", "Open", "High", "Low", "Close",
                                   "Volume", "Change", "PctChange"))
    ## we assign the result in a data.frame with the symbol as first column
    val <- data.frame(symbol=res[[2]], data)
    return(val)
}


## Parameters
symbols <- c("ES1")
host <- "localhost"

redis <- new(Redis, host)
if (redis$ping() != "PONG") stop("No Redis server?", call. = FALSE)

res <- sapply(symbols, redis$subscribe)

repeat {
    rl <- symbolRedisMonitorChannel(redis, type="string")
    if (inherits(rl, "data.frame")) {
        print(rl, row.names=FALSE)
    }
}
