import os.path

os.getcwd()
#print(os.getcwd())

open("teste.txt", "w").close()

os.chdir("A")#mudo de diretorio para salvar arquivo lá dentro da outra pasta!!
#print(os.getcwd())

caminho="/A"


open("morimbundo.txt", "w").close()

print(caminho)