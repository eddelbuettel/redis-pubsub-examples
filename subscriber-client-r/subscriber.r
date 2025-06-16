#!/usr/bin/env r

library(docopt)
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


doc <- "Usage: subscriber.r [--sym SYM] [--host HOST]

Options:
-s --sym SYM   Yahoo! symbol to queyer [default: ^GSPC]
-r --srv HOST  Redis server to connect to [default: localhost]
"

opt <- docopt(doc)

redis <- new(Redis, opt$srv)
if (redis$ping() != "PONG") stop("No Redis server?", call. = FALSE)

res <- sapply(opt$sym, redis$subscribe)

repeat {
    rl <- symbolRedisMonitorChannel(redis, type="string")
    if (inherits(rl, "data.frame")) {
        print(rl, row.names=FALSE)
    }
}
