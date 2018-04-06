package com.gslab.pepper.input.compile;

import javax.tools.JavaCompiler;
import javax.tools.JavaFileObject;
import javax.tools.ToolProvider;
import java.util.Arrays;

/**
 * The InMemoryJavaCompiler uses java compiler to compile classes.
 *
 * Created by trung on 5/3/15.
 *
 * Edited by Satish Bhor<satish.bhor@gslab.com>, Nachiket Kate <nachiket.kate@gslab.com>
 *
 * @Version 1.0
 * @since 5/3/15
 */

public class InMemoryJavaCompiler {

	private InMemoryJavaCompiler() {}
    //Java system compiler
    private static JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();

    public static Class<?> compileSchemaClass(String className, String sourceCodeInText) throws Exception {

        //Java code source Object
        SourceCode sourceCode = new SourceCode(className, sourceCodeInText);

        //Java compiled code object
        CompiledCode compiledCode = new CompiledCode(className);

        //Custom classloader
        DynamicClassLoader dynamicClassLoader = new DynamicClassLoader(InMemoryJavaCompiler.class.getClassLoader());

        //List of source codes to compile
        Iterable<? extends JavaFileObject> compilationUnits = Arrays.asList(sourceCode);

        //Custom file manager takes source code object and classloader
        ExtendedStandardJavaFileManager fileManager = new ExtendedStandardJavaFileManager(compiler.getStandardFileManager(null, null, null), compiledCode, dynamicClassLoader);

        //Compile java code
        JavaCompiler.CompilationTask task = compiler.getTask(null, fileManager, null, null, null, compilationUnits);
        task.call();

         //Load and return class
        return dynamicClassLoader.loadClass(className);


    }
}
