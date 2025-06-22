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

get_data <- function(symbols, tz=defaultTZ) {
    quote <- getQuote(symbols)
    attr(quote$`Trade Time`, "tzone") <- tz
    quote$Close <- quote$Last
    data.frame(Symbols=symbols,
               Time=quote[,"Trade Time"],
               OHLCV(quote),
               Pct_Change = quote[,"% Change"],
               Change = quote[, "Change"])
}

publish_data <- function(vec, redis, symbol) {
    if (redis$ping() == "PONG") {
        for (i in seq_len(nrow(vec))) {
            row <- vec[i,,drop=FALSE]
            txt <- sprintf("%s;%s", row$Time, paste(row[,-(1:2)], collapse=";"))
            symbol <- row$Symbols
            cat(symbol, ":", txt, "\n", sep="")
            redis$publish(symbol, txt, "string")
        }
    }
}

intraday <- function(symbols = c("ES=F", "^GSPC"), sleepdelay = 10, defaultTZ = "America/Chicago") {
    redis <- new(Redis, "localhost")
    if (redis$ping() != "PONG") stop("No Redis server?", call. = FALSE)

    errored <- FALSE
    prevvol <- rep(0, length(symbols))
    repeat {
        curr_t <- Sys.time()
        dat <- try(get_data(symbols, defaultTZ), silent = TRUE)
        if (inherits(dat, "try-error")) {
            msg(curr_t, "Error:", attr(dat, "condition")[["message"]])
            errored <- TRUE
            Sys.sleep(15)
            next
        } else if (errored) {
            errored <- FALSE
            msg(curr_t, "...recovered")
        }
        vol <- Vo(dat)
        volchg <- vol > prevvol
        if (any(volchg)) {
            publish_data(dat[volchg,,drop=FALSE], redis, pubsymbol)
        }
        prevvol <- vol
        Sys.sleep(sleepdelay)
    }
}



doc <- "Usage: multi-symbol-publisher.r [--del SLEEP] [--tz TZ] SYM [SYM [...]]

Options:
-d --del SLEEP  Delay (in seconds) to sleep before next query [default: 10]
-t --tz TZ      Local timezone [default: America/Chicago]
"

opt <- docopt(doc)
intraday(opt$SYM, as.numeric(opt$del), opt$tz)
