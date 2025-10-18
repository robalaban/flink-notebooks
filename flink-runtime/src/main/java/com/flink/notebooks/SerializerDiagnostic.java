package com.flink.notebooks;

/**
 * Diagnostic to test ResultInfoSerializer initialization.
 */
public class SerializerDiagnostic {

    public static void main(String[] args) {
        System.out.println("=== ResultInfoSerializer Diagnostic ===\n");

        try {
            // Try to load the class
            System.out.println("Attempting to load ResultInfoSerializer class...");
            Class<?> clazz = Class.forName("org.apache.flink.table.gateway.rest.serde.ResultInfoSerializer");
            System.out.println("✓ Class loaded successfully: " + clazz.getName());

            // Try to instantiate it
            System.out.println("\nAttempting to create instance...");
            Object instance = clazz.getDeclaredConstructor().newInstance();
            System.out.println("✓ Instance created successfully: " + instance.getClass().getName());

        } catch (ClassNotFoundException e) {
            System.err.println("✗ Class not found: " + e.getMessage());
            e.printStackTrace();
        } catch (NoClassDefFoundError e) {
            System.err.println("✗ NoClassDefFoundError: " + e.getMessage());
            System.err.println("\nThis usually means a static initializer failed or a dependency is missing.");
            System.err.println("Root cause:");
            e.printStackTrace();

            // Try to get the cause
            Throwable cause = e.getCause();
            if (cause != null) {
                System.err.println("\nUnderlying cause:");
                cause.printStackTrace();
            }
        } catch (ExceptionInInitializerError e) {
            System.err.println("✗ Exception in static initializer:");
            e.printStackTrace();

            Throwable cause = e.getCause();
            if (cause != null) {
                System.err.println("\nRoot cause in initializer:");
                cause.printStackTrace();
            }
        } catch (Exception e) {
            System.err.println("✗ Unexpected error: " + e.getMessage());
            e.printStackTrace();
        }

        System.out.println("\n=== Diagnostic complete ===");
    }
}
