#' @description Manage input
#' @param request str:
#' @param error_resp str:
#' @param vals array:
#' @param max_i int:
#' @returns val from `vals` or stops output
request_input <- function(
    request,
    vals,
    error_resp = "Valid input not chosen",
    max_i = 3
){

  message(request)

  i <- 1

  vals_str <- paste0("Please choose one of the following [",
                     paste0(as.character(vals), collapse = "/"),
                     "]:  \n")

  val <- readline(prompt = vals_str)

  while(!val %in% vals){
    val <- readline(prompt = vals_str)
    i <- i + 1
    if(i == max_i){
      stop(error_resp)
    }
  }

}
