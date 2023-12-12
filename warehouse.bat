@echo off
 
set JAVA_HOME="C:\Program Files\Java\jre1.8.0_251"
set PATH=%JAVA_HOME%\bin;%PATH%
 
set JAR_FILE=C:\Users\USER\IdeaProjects\db_warehouse_nhom10\Weather.jar

java -jar %JAR_FILE% %ARGS%

pause