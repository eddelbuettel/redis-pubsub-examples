#!/usr/bin/env r

suppressMessages({
    library(quantmod)
    library(RcppRedis)
})

subsymbol <- "ES=F" 					# Yahoo! symbol
pubsymbol <- "ES1"
defaultTZ <- "America/Chicago"

get_data <- function(symbol, tz=defaultTZ) {
    quote <- getQuote(symbol)
    attr(quote$`Trade Time`, "tzone") <- tz
    quote$Close <- quote$Last
    xts(OHLCV(quote), quote[,"Trade Time"], pct_change = quote[,"% Change"], change = quote[, "Change"])
}

publish_data <- function(vec, redis, symbol) {
    if (redis$ping() == "PONG") {
        ## this is a bit ugly: the xts has print or format method for the elements so we can
        ## use collapse, we then append the two extra values (and we start with the formatted
        ## which only comes in whole seconds)
        txt <- sprintf("%s;%s;%f;%f",
                       format(index(vec)),
                       paste(vec, collapse=";"),
                       attr(vec,"pct_change"),
                       attr(vec,"change"))
        cat(symbol, ":", txt, "\n", sep="")
        redis$publishText(symbol, txt)
    }
}

intraday <- function(symbol = "^GSPC",
                     defaultTZ = "America/Chicago") {

    redis <- new(Redis, "localhost")
    if (redis$ping() != "PONG") stop("No Redis server?", call. = FALSE)

    errored <- FALSE
    prevvol <- 0
    repeat {
        curr_t <- Sys.time()
        dat <- try(get_data(subsymbol, defaultTZ), silent = TRUE)
        if (inherits(dat, "try-error")) {
            msg(curr_t, "Error:", attr(y, "condition")[["message"]])
            errored <- TRUE
            Sys.sleep(15)
            next
        } else if (errored) {
            errored <- FALSE
            msg(curr_t, "...recovered")
        }
        vol <- Vo(dat)[[1]]
        if (vol > prevvol) {
            publish_data(dat, redis, pubsymbol)
        } else {
            #cat("Nope\n")
            #print(y)
        }
        prevvol <- vol
        Sys.sleep(30)
    }
}

intraday()
