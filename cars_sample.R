####################
## R Code Sample Jen T
####################

####
#Set Up
####

#load libraries
pacman::p_load(funModeling, corrplot,
               sqldf, dbplyr,
               RColorBrewer,
               caret, leaps,
               doParallel,
               tidyverse)


#load data locally
mtcars

#store in database
cars_db <- dbConnect(SQLite(), dbname = "Car_Data.sqlite")
dbWriteTable(conn = cars_db, name = "cars_data", value = mtcars, row.names = FALSE, overwrite = T)

#simple database request
dbGetQuery(cars_db, "
   SELECT wt, hp, cyl
   FROM cars_data
   ORDER BY hp DESC
   LIMIT 10
") 

####
# Simple EDA
####

#quick data look
glimpse(mtcars)
df_status(mtcars)

#Add features
mtcars_2 <- mtcars %>% mutate(wt_by_hp = wt * hp,
                              wt_by_carb = wt * carb,
                              gears_per_cyl = gear / cyl)

mtcars_2

#Look at correlation between features and repsonse
correlation_table(data = mtcars_2, target="mpg")

mt_corr <- cor(mtcars_2)
corrplot(mt_corr, type="upper", order="hclust")
#mpg is highly correlated with carb, wt, hp, cyl, and disp so they may all be important variables.
#note hp is highly correlated with a lot of the other varibales, as is wt. We may have issues with collinearity.


#Data Mutation Sample
mtcars_2 %>%
  group_by(cyl) %>%
  mutate(hp_class = case_when(hp < 150 ~ 'low',
                              hp > 150 & hp < 250 ~ 'mid',
                              TRUE ~ 'high')) %>%
  summarise(mean_mpg = mean(mpg),
            mean_wt = mean(wt))

#Sample Plots
mtcars_2 %>%
  select(mpg, wt, hp) %>%
  ggplot(aes(x = mpg, y = wt, color = hp)) +
  geom_point() +
  geom_smooth(method = "lm", se = T) +
  theme_minimal()

mtcars_2 %>%
  select(cyl, mpg) %>%
  group_by(cyl) %>%
  ggplot(aes(x = as.factor(cyl), y = mpg, fill = as.factor(cyl))) +
  geom_boxplot() +
  theme_minimal() +
  scale_fill_brewer(palette="BuPu") +
  labs(x = "cyl", caption = "*Plot of cyl number against mpg")


####
# Modeling
####

#training/testing if needed, but it is a small data set so we'll opt for resampling instead of splitting
#set.seed(123)
#training.samples <- mtcars_2$mpg %>% createDataPartition(p = 0.75, list = FALSE) #from caret package; stratified random sample w/in response variable, ie it keeps the balance
#train.data  <- mtcars[training.samples, ]
#test.data <- mtcars[-training.samples, ]


#Model mpg as a function of the other variables
#Set control method
my_control <- trainControl(
  method = "cv",
  number = 10, # number of k-folds
  savePredictions = "final",
  index=createResample(mtcars_2$mpg, 25),
  allowParallel = TRUE)

#glm model with aic selection
set.seed(123)
glm_mod = train(
  form = mpg ~ ., 
  data = mtcars_2, #data set to build the model on
  trControl = my_control, #perform 10 fold cross validation; method specifies how the sampling should be completed
  method = "glmStepAIC" #glm model with stepAIC feature selection
)

summary(glm_mod)
glm_mod


#lm model with backwards selection
set.seed(123)
lm_mod = train(
  form = mpg ~ ., 
  data = mtcars_2, #data set to build the model on
  trControl = my_control, #perform 10 fold cross validation; method specifies how the sampling should be completed
  method = "leapBackward" #lm model with backwards feature selection
)

summary(lm_mod)
lm_mod


#lm model with stepwise AIC feature selection
set.seed(123)
lm_step_mod = train(
  form = mpg ~ ., 
  data = mtcars_2, #data set to build the model on
  trControl = my_control, #perform 10 fold cross validation; method specifies how the sampling should be completed
  method = "lmStepAIC" #lm model with step AIC feature selection
)

summary(lm_step_mod)
lm_step_mod


#The lm model with backwards feature selection looks like our best model based on the test set, but let's compare visually:
results <- resamples(list(GLM=glm_mod, LMB=lm_mod, LMS=lm_step_mod))
summary(results)
bwplot(results)
dotplot(results)


#If we have any questions about the model selection, we can always compare statistically:
compare_models(glm_mod, lm_mod)
compare_models(lm_step_mod, lm_mod)


#Let's look at the predictions for the LM Backwards model:
predict(lm_mod)

#alternatively, we could build an ensamble of the models and see how that works, but we'll hold off. 
#We also could have selected many different model type.








#################
## Simple Movie Selector Function: find the 3 movies that have a weighted rank similar to the movie of choice
#################

#read in files
registerDoParallel(4)
getDoParWorkers()

setwd("/Users/jstrummer/Documents/.../...") 
infiles <- list.files()


for (val in infiles){
  
  movie_num1 <- scan(val, nlines = 1, what=character())
  
  movie_data <- read.table(val, skip = 1, header = F, sep = ',') %>%
    rename(customer_id = V1,
           rating = V2,
           date = V3) %>%
    mutate(date = as.Date(date),
           movie_num = movie_num1)
  
  dbWriteTable(conn = movies_db, name = paste0("movie_data_", movie_num1), value = movie_data, row.names = FALSE)
  
  rm(movie_num1)
  rm(movie_data)
}

#data processing and exploration removed
movie_selector <- function(movie){
  
  total_ranks <-  all_movies %>%
    left_join(movie_names, by = 'movie_num') %>%
    group_by(movie_num) %>%
    summarise(n = n(),
              avg_rating = mean(rating)) %>%
    ungroup() %>%
    mutate(total_views = sum(n),
           weighted_rating = (n/total_views) * avg_rating) 
  
  selected_rank <- all_movies %>%
    left_join(movie_names, by = 'movie_num') %>%
    group_by(movie_num) %>%
    summarise(n = n(),
              avg_rating = mean(rating)) %>%
    ungroup() %>%
    mutate(total_views = sum(n),
           weighted_rating = (n/total_views) * avg_rating) %>%
    filter(movie_num == movie) %>%
    summarise(upper_rank = weighted_rating + 0.03,
              lower_rank = weighted_rating - 0.03)
  
  upper <- as.numeric(selected_rank$upper_rank)
  lower <- as.numeric(selected_rank$lower_rank)
  
  filter_ranks <- total_ranks %>%
    filter(weighted_rating >= lower & weighted_rating <= upper, movie_num != movie) %>%
    arrange(desc(weighted_rating)) %>%
    top_n(3, weighted_rating) %>%
    select(weighted_rating, movie_num)
  
  return(filter_ranks)
}


