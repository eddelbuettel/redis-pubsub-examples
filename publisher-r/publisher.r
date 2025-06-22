#!/usr/bin/env r

suppressMessages({
    library(docopt)
    library(quantmod)
    library(RcppRedis)
})

msg <- function(ts, ...) {
    op <- options(digits.secs=3)
    cat(format(ts), ..., "\n")
    options(op)
}

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
        redis$publish(symbol, txt, "string")
    }
}

intraday <- function(subsymbol = "^GSPC", pubsymbol = "SP500",
                     sleepdelay = 10, defaultTZ = "America/Chicago") {

    redis <- new(Redis, "localhost")
    if (redis$ping() != "PONG") stop("No Redis server?", call. = FALSE)

    errored <- FALSE
    prevvol <- 0
    repeat {
        curr_t <- Sys.time()
        dat <- try(get_data(subsymbol, defaultTZ), silent = TRUE)
        if (inherits(dat, "try-error")) {
            msg(curr_t, "Error:", attr(dat, "condition")[["message"]])
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
        }
        prevvol <- vol
        Sys.sleep(sleepdelay)
    }
}

doc <- "Usage: publisher.r [--sym SYM] [--pub PUB] [--del SLEEP] [--tz TZ]

Options:
-s --sym SYM	Yahoo! symbol to queyer [default: ^GSPC]
-p --pub PUB    Symbol to publish via Redis [default: SP500]
-d --del SLEEP  Delay (in seconds) to sleep before next query [default: 10]
-t --tz TZ      Local timezone [default: America/Chicago]
"

opt <- docopt(doc)
intraday(opt$sym, opt$pub, as.numeric(opt$del), opt$tz)
