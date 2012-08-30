library("rjson")

FluentLogger <- setRefClass(
  "FluentLogger",
  
  fields=c("tag", "host", "port", "timeout", "sender"),
  
  methods=list(
    
    post=function(label, data, timestamp=as.integer(Sys.time())) {
      tryCatch({
          jsondata <- toJSON(list(paste(tag, label, sep="."), timestamp, data))
          writeLines(jsondata, sender)
          .self$flush()
          invisible(TRUE)
        },
        warning=function(w) { message(w); invisible(FALSE) },
        error=function(e) { message(e); invisible(FALSE) }
      )
    },
    
    close=function() {
      tryCatch({
          if (isOpen(sender)) {
            close.connection(sender)
            sender <<- NULL
            
            # delete from secret object .loggers
            objName <- ".loggers"
            loggers <- get(objName, envir=FluentLoggerEnv)
            loggers[[paste(tag, host, port, timeout, sep="_")]] <- NULL
            assign(objName, loggers, envir=FluentLoggerEnv)
          }
        },
        warning=function(w) { message(w) }, # ignore
        error=function(e) { message(e) } # ignore
      )
    },
    
    flush=function() {
      tryCatch({
          if (isIncomplete(sender)) { 
            flush.connection(sender) 
          }
        },
        warning=function(w) { message(w) }, # ignore
        error=function(e) { message(e) } # ignore
      )
    }
  )
)

getFluentLogger <- function(tag, host, port=24224, timeout=3) {
  
  # if there aren't exist the environment FluentLoggerEnv
  if (!exists("FluentLoggerEnv", envir=.GlobalEnv)) {
    # make the envir.
    FluentLoggerEnv <- new.env(parent=.GlobalEnv)
    assign("FluentLoggerEnv", FluentLoggerEnv, envir=.GlobalEnv)
  }
  
  # get secret object .loggers from FluentLoggerEnv
  objName <- ".loggers"
  if (exists(objName, envir=FluentLoggerEnv)) {
    loggers <- get(objName, envir=FluentLoggerEnv)
  } else {
    loggers <- list()
  }
  
  # search logger by tag, host, port and timeout.
  key <- paste(tag, host, port, timeout, sep="_")
  if (is.null(loggers[[key]])) {
    # if loggers doesn't contain the key, make new FluentLogger.
    tryCatch(
      sender <- socketConnection(host, port, timeout=timeout),
      warning=function(w) stop(paste("Failed to connect fluentd:", paste(host, port, sep=":")), call.=F),
      error=function(e) stop(paste("Failed to connect fluentd:", paste(host, port, sep=":")), call.=F)
    )
    loggers[[key]] <- FluentLogger$new(tag=tag, host=host, port=port, timeout=timeout, sender=sender)
    assign(objName, loggers, envir=FluentLoggerEnv)
  }
  
  loggers[[key]]
}

closeAll <- function() {
  objName <- ".loggers"
  if (exists("FluentLoggerEnv", envir=.GlobalEnv) && exists(objName, envir=FluentLoggerEnv)) {
    loggers <- get(objName, envir=FluentLoggerEnv)
    for (logger in loggers) {
      logger$close()
      logger <- NULL
    }
    # remove .loggers from FluentLoggerEnv
    rm(".loggers", envir=FluentLoggerEnv)
  }
}

flushAll <- function() {
  objName <- ".loggers"
  if (exists("FluentLoggerEnv", envir=.GlobalEnv) && exists(objName, envir=FluentLoggerEnv)) {
    loggers <- get(objName, envir=FluentLoggerEnv)
    for (logger in loggers) {
      logger$flush()
    }
  }
}