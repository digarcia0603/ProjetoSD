Se a pasta bin não tiver os ficheiros ".class", criar primeiro:
>>> javac -d bin src/main/java/org/example/*.java src/main/java/org/example/utils/*.java
>>> javac -d bin -cp bin:lib/jfreechart-1.5.6.jar src/test/java/TimeSeriesRunner.java
       
Para correr o servidor:
>>> java -cp bin org.example.Server 30

Para correr a interface do utilizador (Server tem de estar iniciado):
>>> java -cp bin org.example.ClientUI

Para correr os testes de desempenho (Server tem de estar iniciado):
>>> java -cp bin:lib/jfreechart-1.5.6.jar TimeSeriesRunner
