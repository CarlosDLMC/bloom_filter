For using external pyspark libraries with pyspark:

-Create a new folder 
 mkdir libs

-Pip install the library with target, on that library
 cd libs
 pip install --target . <LIBRARY>

- Compress that library
  zip -r . library.zip

- Add that library at the spark-submit execution.
 * If the library is pure python, just add it on --py-files
 * If it has some conf files or dynamic libraries, add it on --py-files and on --archives
