subset_similar_test <- function(filepath, buoy_data, train_filepath){
  train_dates <- feather::read_feather(train_filepath) %>% pull(DateTime) %>% unique()
  buoy_data %>% filter(!DateTime %in% train_dates) %>%
    feather::write_feather(path = filepath)
}

build_sparse_training <- function(filepath, buoy_data, chunksize){
  details <- basename(filepath) %>% strsplit('[_]') %>% .[[1]]
  exp_n <- tail(details,1) %>% strsplit('[.]') %>% .[[1]] %>% .[1]
  prof_n <- as.numeric(details[4])



  un_dates <- buoy_data %>% pull(DateTime) %>% unique

  un_dt_resample <- data.frame(date = un_dates, train = FALSE)
  set.seed(42 + as.numeric(exp_n))
  while (sum(un_dt_resample$train) + chunksize < prof_n){
    good_sample <- FALSE
    while (!good_sample){
      start_i <- sample(1:nrow(un_dt_resample), 1)
      end_i <- start_i + chunksize - 1
      if (end_i <= nrow(un_dt_resample) & !any(un_dt_resample$train[start_i:end_i])){
        good_sample = TRUE
        un_dt_resample$train[start_i:end_i] <- TRUE
      }
    }
  }

  # now get the rest, which will be smaller or equal to chunksize
  chunksize <- prof_n - sum(un_dt_resample$train)
  good_sample <- FALSE
  while (!good_sample){
    start_i <- sample(1:nrow(un_dt_resample), 1)
    end_i <- start_i + chunksize - 1
    if (end_i <= nrow(un_dt_resample) & !any(un_dt_resample$train[start_i:end_i])){
      good_sample = TRUE
      un_dt_resample$train[start_i:end_i] <- TRUE
    }
  }

  buoy_data %>% filter(DateTime %in% un_dt_resample$date[un_dt_resample$train]) %>%
    feather::write_feather(path = filepath)
}


plot_test_train <- function(filepath, xlim_char, ...){

  files <- c(...)
  year_ticks <- seq(as.Date(c("2007-01-1")), length.out = 20, by = 'year')
  png(filename = filepath, width = 14, height = 4.5, units = 'in', res = 200)
  par(omi = c(0.25,0,0.05,0.05), mai = c(0.05,0.5,0,0), las = 1, mgp = c(2,.5,0))
  layout(matrix(1:length(files)))

  xlim <- as.Date(xlim_char)

  for (file in files){
    plot(xlim, c(NA,NA), xlim = xlim,
         ylim = c(24,0), xaxs = 'i', yaxs = 'i', ylab = 'Depth (m)', xlab = "", axes = FALSE)

    axis(1, at = year_ticks, labels = FALSE, tick = TRUE, tck = -0.02)
    axis(2, at = seq(0,25,10), tck = -0.02)

    temp_data <- feather::read_feather(file)
    un_dates <- temp_data$DateTime %>% unique()
    for (date in un_dates){
      data <- filter(temp_data, DateTime == date) %>% arrange(Depth)
      data <- data[!duplicated(data$Depth), ]
      if (nrow(data) > 2){
        col = colorRampPalette(c("#00007F", "blue", "#007FFF", "cyan",
                                 "#7FFF7F", "yellow", "#FF7F00", "red", "#7F0000"))(30)
        .filled.contour(x = c(date-0.5, date+0.5), y = data$Depth, z = rbind(data$temp,data$temp), levels = seq(0,to = 30), col = col)

      }

    }
    box()
    details <- basename(file) %>% strsplit('[_]') %>% .[[1]]
    if (any(grepl(pattern = 'test', details))){
      inset_txt = "test n=%s"
    } else {
      inset_txt = "training n=%s"
    }
    text(xlim[1]+10, y = 4, sprintf(inset_txt, length(un_dates)), pos = 4)
    text(xlim[1]+10, y = 20, basename(file), pos = 4)
  }
  axis(1, at = year_ticks, labels = seq(2007, by= 1, length.out = 20), tick = TRUE, tck = -0.02)
  dev.off()
}

plot_similar_test_train <- function(filepath, xlim_char, ...){

  files <- c(...)
  year_ticks <- seq(as.Date(c("2007-01-1")), length.out = 20, by = 'year')
  png(filename = filepath, width = 14, height = 4.2, units = 'in', res = 200)
  par(omi = c(0.25,0,0.05,0.05), mai = c(0.05,0.5,0,0), las = 1, mgp = c(2,.5,0))
  layout(matrix(1:(length(files)/2)))

  xlim <- as.Date(xlim_char)
  test_files <- files[seq(2,by = 2, to = length(files))]
  train_files <- files[seq(1,by = 2, to = length(files))]

  for (file in train_files){
    plot(xlim, c(NA,NA), xlim = xlim,
         ylim = c(24,0), xaxs = 'i', yaxs = 'i', ylab = 'Depth (m)', xlab = "", axes = FALSE)

    axis(1, at = year_ticks, labels = FALSE, tick = TRUE, tck = -0.02)
    axis(2, at = seq(0,25,10), tck = -0.02)

    temp_data <- feather::read_feather(test_files[1])

    un_dates <- temp_data$DateTime %>% unique()
    for (date in un_dates){
      data <- filter(temp_data, DateTime == date) %>% arrange(Depth)
      data <- data[!duplicated(data$Depth), ]
      if (nrow(data) > 2){
        col = colorRampPalette(c("#00007F", "blue", "#007FFF", "cyan",
                                 "#7FFF7F", "yellow", "#FF7F00", "red", "#7F0000"))(30)
        .filled.contour(x = c(date-0.5, date+0.5), y = data$Depth, z = rbind(data$temp,data$temp), levels = seq(0,to = 30), col = col)
        .filled.contour(x = c(date-0.5, date+0.5), y = c(0, 24), z = rbind(c(1,1),c(1,1)), levels = seq(0,to = 30), col = '#0000004D')
      }

    }

    temp_data <- feather::read_feather(file)
    un_dates <- temp_data$DateTime %>% unique()
    for (date in un_dates){
      data <- filter(temp_data, DateTime == date) %>% arrange(Depth)
      data <- data[!duplicated(data$Depth), ]
      if (nrow(data) > 2){
        col = colorRampPalette(c("#00007F", "blue", "#007FFF", "cyan",
                                 "#7FFF7F", "yellow", "#FF7F00", "red", "#7F0000"))(30)
        .filled.contour(x = c(date-0.5, date+0.5), y = data$Depth, z = rbind(data$temp,data$temp), levels = seq(0,to = 30), col = col)

      }

    }

    box()
    details <- basename(file) %>% strsplit('[_]') %>% .[[1]]
    if (any(grepl(pattern = 'test', details))){
      inset_txt = "test n=%s"
    } else {
      inset_txt = "training n=%s"
    }
    text(xlim[1]+10, y = 4, sprintf(inset_txt, length(un_dates)), pos = 4)
    text(xlim[1]+10, y = 21.5, basename(file), pos = 4)
    text(xlim[1]+10, y = 17, basename(test_files[1]), pos = 4)
    test_files <- tail(test_files, -1L)
  }
  axis(1, at = year_ticks, labels = seq(2007, by= 1, length.out = 20), tick = TRUE, tck = -0.02)
  dev.off()
}

write_hypsography <- function(target_name, nhd_id){
  write_csv(x = lakeattributes::get_bathy(nhd_id), target_name)
}

subset_GLM_meteo <- function(filepath, filepath_in, range){
  readr::read_csv(filepath_in) %>%
    filter(time <= as.Date(range[2]), time >= as.Date(range[1])) %>%
    feather::write_feather(path = filepath)
}
