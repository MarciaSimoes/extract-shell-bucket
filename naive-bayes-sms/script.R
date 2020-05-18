setwd("C:\\Users\\marcia.cunha\\Documents\\analise_preditiva\\Rnapratica\\AULA_2")
getwd()
#install packges
install.packages("tm")
install.packages("SnowballC")
install.packages("wordcloud")
install.packages("e1071")
install.packages("gmodels")
install.packages("tm")  # for text mining
install.packages("wordcloud") # word-cloud generator 
install.packages("RColorBrewer") # color palettes
# Load


library("wordcloud")
library("RColorBrewer")
library("tm")
library("SnowballC")
library("wordcloud")
library("e1071")
library("gmodels")

sms_df <-read.csv("sms_spam.csv",stringsAsFactors = FALSE)
View(sms_df)

#criando uma variavel categorica para verificar quantas msg spam e nao spam
sms_df$type<-factor(sms_df$type)
table(sms_df$type)
View(sms_df)


#vou 4812 msg nao spam = ham e 747 spam

#trabalhando com a biblioteca tm data mining
#criei um objeto do tipo coups para mineração de dados
#criando uma coleção de dados para mineração de dados usa a library "tm" ela mostra os metadados

View(sms_corpus)

#aplicando metodo paralimpeza dos dados
#exemplo converter em mnuscula, limpar caracteres etc usa library tm
sms_corpus_clean<-tm_map(sms_corpus,content_transformer(tolower))
View(sms_corpus_clean)

#limpando numeros
sms_corpus_clean<-tm_map(sms_corpus_clean,content_transformer(removeNumbers))
View(sms_corpus_clean)

#visualizando Stop words ou seja palavras comuns que nao vamos usar
stopwords(kind = "pt")
sms_corpus_clean<-tm_map(sms_corpus_clean,content_transformer(stopwords(kind = "pt")))
View(sms_corpus_clean)

#removendo pontuações
sms_corpus_clean<-tm_map(sms_corpus_clean,content_transformer(removePunctuation))
View(sms_corpus_clean)

#removendo espaçoes em branco
sms_corpus_clean<-tm_map(sms_corpus_clean,content_transformer(stripWhitespace))
View(sms_corpus_clean)

#removendo sufixos das palavras
wordStem(c("learn","learned","learning","learn"))
sms_corpus_clean<-tm_map(sms_corpus_clean,stemDocument)
View(sms_corpus_clean)

#comparando as bases antes e deposi da limpeza
lapply(sms_corpus[1:3],as.character)
lapply(sms_corpus_clean[1:3],as.character)


#visualizacao das palavras encontradas na base biblioteca owrdcloud
set.seed(1234)
wordcloud(words = sms_corpus_clean$text, freq = sms_corpus_clean$freq, min.freq = 1,
          max.words=200, random.order=FALSE, rot.per=0.35, 
          colors=brewer.pal(8, "Dark2"))