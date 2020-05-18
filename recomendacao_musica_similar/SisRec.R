setwd("C:\\Users\\marcia.cunha\\Documents\\analise_preditiva\\RnaPratica\\") 
getwd()
base <- read.csv(file="lastfm.csv")
head(base)
View(base)
class(base) #tipo
#Verificando as dimensões do data frame
dim(base)
#Visualizando a estrutura do data frame
str(base)

#Verificando os nomes das variáveis
colnames(base)
#Obtendo os resultados das primeiras linhas e das colunas 1, 4, 5, 6, 7 e
head(base[,c(1,4:8)])

base.msc <- (base[,!(names(base) %in% c("user"))])
#Confirmando a retirada da coluna "user"
str(base.msc)
calculaCosseno<- function(x,y)
{
  cosseno <- sum(x*y) / (sqrt(sum(x*x)) * sqrt(sum(y*y)))
  return(cosseno)
}

# pesquisar library que tratar dados nulos pontos??
# pesquisar particionamento de arquivo


basecosseno <- matrix(NA, nrow=ncol(base.msc), ncol=ncol(base.msc),
                        dimnames=list(colnames(base.msc), colnames(base.msc)))
View(basecosseno)

#Calcular a similaridade entre todas as colunas em uma matriz

for(i in 1:ncol(base.msc)) {
  for(j in 1:ncol(base.msc)) {
    basecosseno[i,j] <- calculaCosseno(as.matrix(base.msc[i]), as.matrix(base.msc[j]))
  }
}
#Converter a matriz de similaridade em um data frame

basecosseno <- as.data.frame(basecosseno)

#Visualizar o conteúdo dos primeiros registros do data frame
head(basecosseno[,c(1,2:5)])

#Criar uma nova matriz para armazenar os top-10 vizinhos mais
#próximos de cada item


basevizinhos <- matrix(NA, nrow=ncol(basecosseno),
                       ncol=11, dimnames=list(colnames(basecosseno)))

#Ordenar a matriz em ordem decrescente de similaridade
for(i in 1:ncol(base.msc)) {
  basevizinhos[i,] <- (t(head(n=11, rownames(basecosseno[order(
    basecosseno[,i], decreasing=TRUE),][i]))));
}

View(basevizinhos)


#Gravar o resultado em um arquivo csv

write.csv(file="top10lastfm.csv",x=basevizinhos[,-1])
