import { Client } from "cassandra-driver";
import dotenv from "dotenv";
dotenv.config();

class CassandraService {
    private client: Client;
    private keyspace: string;

    constructor() {
        this.client = new Client({
            contactPoints: [process.env.CASSANDRA_HOST || "localhost"],
            localDataCenter: "datacenter1",
        });
        this.keyspace = "mis_keyspace";
    }

    async connect() {
        await this.client.connect();
        console.log("✅ Connected to Cassandra");

        await this.client.execute(`
            CREATE KEYSPACE IF NOT EXISTS ${this.keyspace}
            WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};
        `);
        console.log("✅ Keyspace ready");
    }

    async disconnect() {
        await this.client.shutdown();
        console.log("🔌 Disconnected from Cassandra");
    }

    // Task 1: Create Table with Composite Primary Key
    async createTable() {
        await this.client.execute(`
            CREATE TABLE IF NOT EXISTS ${this.keyspace}.employees (
                department TEXT,
                hire_date DATE,
                employee_id UUID,
                name TEXT,
                salary INT,
                PRIMARY KEY (department, hire_date, employee_id)
            ) WITH CLUSTERING ORDER BY (hire_date DESC, employee_id ASC);
        `);
        await this.client.execute(`TRUNCATE ${this.keyspace}.employees`)
        console.log("Table created successfully");
    }

    // Task 2: Insert 5 Rows
    async insertRows() {
        const rows = [
            { department: "Sales", hire_date: "2026-01-01", name: "Alice", salary: 5000 },
            { department: "Sales", hire_date: "2026-02-01", name: "Bob",   salary: 6000 },
            { department: "IT",    hire_date: "2026-03-01", name: "Charlie", salary: 7000 },
            { department: "IT",    hire_date: "2026-04-01", name: "David", salary: 8000 },
            { department: "IT",    hire_date: "2026-05-01", name: "Eve",   salary: 9000 },
        ];

        for (const row of rows) {
            await this.client.execute(
                `INSERT INTO ${this.keyspace}.employees 
                 (department, hire_date, employee_id, name, salary) 
                 VALUES (?, ?, uuid(), ?, ?)`,
                [row.department, row.hire_date, row.name, row.salary],
                { prepare: true }
            );
        }
        console.log("5 rows inserted successfully");
    }

    // Task 3a: Update an employee's salary by their full primary key
    async updateSalary(department: string, hireDate: string, employeeId: string, newSalary: number) {
        await this.client.execute(
            `UPDATE ${this.keyspace}.employees 
             SET salary = ? 
             WHERE department = ? AND hire_date = ? AND employee_id = ?`,
            [newSalary, department, hireDate, employeeId],
            { prepare: true }
        );
        console.log(`Updated salary to ${newSalary} for employee ${employeeId}`);
    }

    // Task 3b: Delete an employee by their full primary key
    async deleteEmployee(department: string, hireDate: string, employeeId: string) {
        await this.client.execute(
            `DELETE FROM ${this.keyspace}.employees 
             WHERE department = ? AND hire_date = ? AND employee_id = ?`,
            [department, hireDate, employeeId],
            { prepare: true }
        );
        console.log(`Deleted employee ${employeeId} from ${department}`);
    }

    // Helper: Fetch all rows (useful for grabbing employee_ids after insert)
    async getAllEmployees() {
        const result = await this.client.execute(
            `SELECT department, hire_date, employee_id, name, salary FROM ${this.keyspace}.employees`
        );
        return result.rows;
    }
}

// --- Run ---
async function main() {
    const service = new CassandraService();
    try {
        await service.connect();
        await service.createTable();
        await service.insertRows();

        // Fetch inserted rows to get real employee_ids for update/delete
        const employees = await service.getAllEmployees();
        console.log("Employees:", employees.map(e => ({ name: e.name, id: e.employee_id, dept: e.department, hire_date: e.hire_date })));

        // Example: update Bob's salary (grab his ID from results)
        const bob = employees.find(e => e.name === "Bob");
        if (bob) {
            await service.updateSalary(bob.department, bob.hire_date.toString(), bob.employee_id.toString(), 6500);
        }

        // Example: delete Alice's record
        const alice = employees.find(e => e.name === "Alice");
        if (alice) {
            await service.deleteEmployee(alice.department, alice.hire_date.toString(), alice.employee_id.toString());
        }

    } finally {
        await service.disconnect();
    }
}

main();