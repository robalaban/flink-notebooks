package com.flink.notebooks;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.gateway.service.SqlGatewayServiceImpl;
import org.apache.flink.table.gateway.service.context.DefaultContext;
import org.apache.flink.table.gateway.service.session.SessionManagerImpl;

import java.lang.reflect.Field;
import java.util.Collections;

/**
 * Diagnostic tool to inspect OperationManager fields in Flink 1.20.0.
 * Helps in debuging
 */
public class OperationManagerDiagnostic {

    public static void main(String[] args) throws Exception {
        System.out.println("=== SqlGatewayServiceImpl Field Inspector ===\n");

        Configuration config = new Configuration();
        DefaultContext defaultContext = DefaultContext.load(config, Collections.emptyList(), false);
        SessionManagerImpl sessionManager = new SessionManagerImpl(defaultContext);
        SqlGatewayServiceImpl gatewayService = new SqlGatewayServiceImpl(sessionManager);

        System.out.println("SqlGatewayServiceImpl class: " + gatewayService.getClass().getName());
        System.out.println("\nDeclared fields in SqlGatewayServiceImpl:");

        Field[] gatewayFields = SqlGatewayServiceImpl.class.getDeclaredFields();
        Object sessionManagerObj = null;
        for (Field field : gatewayFields) {
            field.setAccessible(true);
            Object value = field.get(gatewayService);
            String valueStr = (value == null) ? "null" : value.getClass().getSimpleName();
            System.out.printf("  %-40s %-30s (current value: %s)%n",
                field.getName(),
                field.getType().getSimpleName(),
                valueStr
            );
            if (field.getName().equals("sessionManager")) {
                sessionManagerObj = value;
            }
        }

        // Inspect SessionManager
        if (sessionManagerObj != null) {
            System.out.println("\n=== SessionManager Implementation ===");
            System.out.println("Class: " + sessionManagerObj.getClass().getName());
            System.out.println("\nDeclared fields:");

            Field[] sessionFields = sessionManagerObj.getClass().getDeclaredFields();
            for (Field field : sessionFields) {
                field.setAccessible(true);
                Object value = field.get(sessionManagerObj);
                String valueStr = (value == null) ? "null" : value.getClass().getSimpleName();
                System.out.printf("  %-40s %-30s (current value: %s)%n",
                    field.getName(),
                    field.getType().getSimpleName(),
                    valueStr
                );

                if (field.getType().getSimpleName().contains("Operation") && value != null) {
                    System.out.println("\n    >>> Found OperationManager: " + field.getName());
                    System.out.println("    >>> Class: " + value.getClass().getName());
                    System.out.println("    >>> Fields:");
                    Field[] innerFields = value.getClass().getDeclaredFields();
                    for (Field innerField : innerFields) {
                        innerField.setAccessible(true);
                        Object innerValue = innerField.get(value);
                        String innerValueStr = (innerValue == null) ? "null" : innerValue.getClass().getSimpleName();
                        System.out.printf("      %-38s %-28s (current value: %s)%n",
                            innerField.getName(),
                            innerField.getType().getSimpleName(),
                            innerValueStr
                        );
                    }
                    System.out.println();
                }
            }
        }

        System.out.println("\n=== Field inspection complete ===");
    }
}
