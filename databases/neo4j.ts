import neo4j, { Driver, Session } from "neo4j-driver";
import dotenv from "dotenv";
import path from "path";
import { fileURLToPath } from "url";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
dotenv.config({ path: path.resolve(__dirname, "../../.env") });

// Dataset: Friendship Network
// Nodes: Person (name, age, city)
// Relationships: FRIENDS_WITH (since, closeness), WORKS_WITH (company, role)

class Neo4jService {
    private driver: Driver;

    constructor() {
        this.driver = neo4j.driver(
            process.env.NEO4J_URI || "bolt://localhost:7687",
            neo4j.auth.basic(
                process.env.NEO4J_USER || "neo4j",
                process.env.NEO4J_PASSWORD || "password123"
            )
        );
    }

    private getSession(): Session {
        return this.driver.session();
    }

    async close() {
        await this.driver.close();
        console.log("Disconnected from Neo4j");
    }

    // Task 1: Create nodes, relationships, and properties
    async createGraph() {
        const session = this.getSession();
        try {
            // Clear existing data
            await session.run(`MATCH (n) DETACH DELETE n`);

            // Create 6 Person nodes
            await session.run(`
                CREATE (alice:Person   {id: 1, name: 'Alice',   age: 30, city: 'New York'})
                CREATE (bob:Person     {id: 2, name: 'Bob',     age: 25, city: 'Los Angeles'})
                CREATE (charlie:Person {id: 3, name: 'Charlie', age: 35, city: 'New York'})
                CREATE (diana:Person   {id: 4, name: 'Diana',   age: 28, city: 'Chicago'})
                CREATE (eve:Person     {id: 5, name: 'Eve',     age: 22, city: 'Los Angeles'})
                CREATE (frank:Person   {id: 6, name: 'Frank',   age: 40, city: 'New York'})

                // FRIENDS_WITH relationships
                CREATE (alice)-[:FRIENDS_WITH   {since: 2018, closeness: 'close'}]->(bob)
                CREATE (alice)-[:FRIENDS_WITH   {since: 2015, closeness: 'best'}]->(charlie)
                CREATE (bob)-[:FRIENDS_WITH     {since: 2020, closeness: 'acquaintance'}]->(diana)
                CREATE (charlie)-[:FRIENDS_WITH {since: 2019, closeness: 'close'}]->(eve)
                CREATE (diana)-[:FRIENDS_WITH   {since: 2021, closeness: 'close'}]->(eve)
                CREATE (frank)-[:FRIENDS_WITH   {since: 2016, closeness: 'close'}]->(alice)

                // WORKS_WITH relationships
                CREATE (alice)-[:WORKS_WITH {company: 'TechCorp', role: 'colleague'}]->(charlie)
                CREATE (bob)-[:WORKS_WITH   {company: 'DataInc',  role: 'manager'}]->(diana)
            `);

            console.log("Task 1 - Graph created: 6 Person nodes, FRIENDS_WITH and WORKS_WITH relationships");
        } finally {
            await session.close();
        }
    }

    // Task 2: Delete nodes, relationships, and properties
    async deleteNodesAndRelationships() {
        const session = this.getSession();
        try {
            // 2a. Remove a property from a node
            await session.run(`
                MATCH (p:Person {name: 'Frank'})
                REMOVE p.city
            `);
            console.log("Task 2a - Removed 'city' property from Frank");

            // 2b. Delete a specific relationship
            await session.run(`
                MATCH (bob:Person {name: 'Bob'})-[r:WORKS_WITH]->(diana:Person {name: 'Diana'})
                DELETE r
            `);
            console.log("Task 2b - Deleted WORKS_WITH relationship between Bob and Diana");

            // 2c. Delete a node entirely (DETACH removes its relationships too)
            await session.run(`
                MATCH (p:Person {name: 'Frank'})
                DETACH DELETE p
            `);
            console.log("Task 2c - Deleted Frank node and all his relationships");
        } finally {
            await session.close();
        }
    }

    // Task 3: Update properties of nodes and relationships
    async updateProperties() {
        const session = this.getSession();
        try {
            // Update node properties
            await session.run(`
                MATCH (p:Person {name: 'Alice'})
                SET p.age = 31, p.email = 'alice@example.com'
            `);

            await session.run(`
                MATCH (p:Person {name: 'Bob'})
                SET p.city = 'San Francisco'
            `);

            // Update a relationship property
            await session.run(`
                MATCH (a:Person {name: 'Alice'})-[r:FRIENDS_WITH]->(b:Person {name: 'Bob'})
                SET r.closeness = 'best'
            `);

            console.log("Task 3 - Updated Alice (age, email), Bob (city), and Alice→Bob friendship closeness");
        } finally {
            await session.close();
        }
    }

    // Task 4: Find nodes based on a condition
    async findNodes() {
        const session = this.getSession();
        try {
            // 4a. Find people living in New York
            const nyResult = await session.run(`
                MATCH (p:Person) WHERE p.city = 'New York'
                RETURN p.name AS name, p.age AS age
            `);
            console.log("Task 4a - People in New York:");
            nyResult.records.forEach(r =>
                console.log(`  - ${r.get("name")}, age: ${r.get("age")}`)
            );

            // 4b. Find people older than 27
            const ageResult = await session.run(`
                MATCH (p:Person) WHERE p.age > 27
                RETURN p.name AS name, p.age AS age ORDER BY p.age DESC
            `);
            console.log("Task 4b - People older than 27:");
            ageResult.records.forEach(r =>
                console.log(`  - ${r.get("name")}, age: ${r.get("age")}`)
            );
        } finally {
            await session.close();
        }
    }

    // Task 5: Find relationships based on a condition
    async findRelationships() {
        const session = this.getSession();
        try {
            // 5a. Find all friendships formed before 2020
            const oldFriends = await session.run(`
                MATCH (a:Person)-[r:FRIENDS_WITH]->(b:Person)
                WHERE r.since < 2020
                RETURN a.name AS person1, b.name AS person2, r.since AS since, r.closeness AS closeness
            `);
            console.log("Task 5a - Friendships formed before 2020:");
            oldFriends.records.forEach(r =>
                console.log(`  - ${r.get("person1")} → ${r.get("person2")} (since ${r.get("since")}, ${r.get("closeness")})`)
            );

            // 5b. Find only 'best' or 'close' friendships
            const closeFriends = await session.run(`
                MATCH (a:Person)-[r:FRIENDS_WITH]->(b:Person)
                WHERE r.closeness IN ['best', 'close']
                RETURN a.name AS person1, b.name AS person2, r.closeness AS closeness
            `);
            console.log("Task 5b - Best/Close friendships:");
            closeFriends.records.forEach(r =>
                console.log(`  - ${r.get("person1")} → ${r.get("person2")} (${r.get("closeness")})`)
            );
        } finally {
            await session.close();
        }
    }
}

// Run
async function main() {
    const service = new Neo4jService();
    try {
        await service.createGraph();
        await service.deleteNodesAndRelationships();
        await service.updateProperties();
        await service.findNodes();
        await service.findRelationships();
        console.log("\nAll Neo4j tasks completed!");
    } finally {
        await service.close();
    }
}

main();