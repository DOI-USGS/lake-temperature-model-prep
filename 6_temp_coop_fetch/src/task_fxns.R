create_coop_taskplan <- function(wants) {

  coop_fetch_step1 <- scipiper::create_task_step(
    step_name = 'check_and_write_ind',
    target_name = function(task_name, ...) {
      scipiper::as_ind_file(file.path('6_temp_coop_fetch/in', task_name))
    },
    command = "gd_confirm_posted(ind_file = target_name)"
  )

  coop_fetch_step2 <- scipiper::create_task_step(
    step_name = 'download',
    target_name = function(task_name, ...) {
      file.path('6_temp_coop_fetch/in', task_name)
    },
    command = function(target_name, ...) {
      sprintf("gd_get('%s')", scipiper::as_ind_file(target_name))
    }
  )

  task_plan <- scipiper::create_task_plan(
    task_names = wants,
    task_steps = list(coop_fetch_step1, coop_fetch_step2),
    final_steps = 'check_and_write_ind',
    add_complete = FALSE
  )

  return(task_plan)
}

create_coop_fetch_makefile <- function(target_name, taskplan, final_targets) {
  create_task_makefile(
    makefile = target_name,
    task_plan = taskplan,
    include = '6_temp_coop_fetch.yml',
    packages = c('scipiper'),
    file_extensions = c("ind"),
    final_targets = final_targets
  )
}
