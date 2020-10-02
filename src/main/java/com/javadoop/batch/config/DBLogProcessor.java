package com.javadoop.batch.config;

import org.springframework.batch.item.ItemProcessor;

import com.javadoop.batch.entity.Employee;

public class DBLogProcessor implements ItemProcessor<Employee, Employee> {
    public Employee process(Employee employee) throws Exception {
        System.out.println("Inserting employee : " + employee);
        return employee;
    }
}
