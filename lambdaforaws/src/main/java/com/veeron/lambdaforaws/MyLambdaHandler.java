package com.veeron.lambdaforaws;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cloud.function.context.FunctionRegistry;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.function.Function;


public class MyLambdaHandler implements RequestHandler<Map<String, Object>, String> {

    @Autowired
    private FunctionRegistry functionRegistry;

    @Override
    public String handleRequest(Map<String, Object> event, Context context) {
       
        String functionName = (String) event.get("functionName"); 

      
        Function<String, String> function = (Function<String, String>) functionRegistry.lookup(functionName);

        if (function == null) {
            return "Function not found: " + functionName;
        }

       
        String inputData = (String) event.get("data");

        
        return function.apply(inputData);
    }
}
