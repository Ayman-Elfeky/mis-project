import { MongoClient, Collection } from "mongodb";
import dotenv from "dotenv";
import path from "path";
import { fileURLToPath } from "url";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
dotenv.config({ path: path.resolve(__dirname, "../../.env") });

// Interfaces 

interface Student {
    _id: number;
    name: string;
    enrolledCourseIds: number[];
    Score?: number[];
}

interface Instructor {
    _id: number;
    name: string;
    department: string;
    Score?: number[];
}

interface Course {
    _id: number;
    title: string;
    instructorId: number;
}

// Service 

class MongoService {
    private client: MongoClient;
    private students!: Collection<Student>;
    private instructors!: Collection<Instructor>;
    private courses!: Collection<Course>;

    constructor() {
        const uri = process.env.MONGO_URI || "mongodb://127.0.0.1:27017";
        this.client = new MongoClient(uri);
    }

    async connect() {
        await this.client.connect();
        const db = this.client.db("mis_project");
        this.students    = db.collection<Student>("students");
        this.instructors = db.collection<Instructor>("instructors");
        this.courses     = db.collection<Course>("courses");
        console.log("Connected to MongoDB");
    }

    async disconnect() {
        await this.client.close();
        console.log("Disconnected from MongoDB");
    }

    // Task 1: Insert 3 documents in each collection 
    async insertDocuments() {
        // Clear all two collections first
        await this.students.deleteMany({});
        await this.instructors.deleteMany({});

        // 3 Instructors
        await this.instructors.insertMany([
            { _id: 1, name: "Dr. Smith",   department: "Computer Science" },
            { _id: 2, name: "Dr. Johnson", department: "Mathematics" },
            { _id: 3, name: "Dr. Lee",     department: "Information Systems" },
        ]);

        // 3 Students
        await this.students.insertMany([
            { _id: 1, name: "Alice",   enrolledCourseIds: [101, 102] },
            { _id: 2, name: "Bob",     enrolledCourseIds: [101, 103] },
            { _id: 3, name: "Charlie", enrolledCourseIds: [102] },
        ]);


        console.log("Task 1 — Inserted 3 instructors, 3 students");
    }

    // Task 2: Delete one document from each collection 
    async deleteOneEach() {
        await this.students.deleteOne({ _id: 3 });        
        await this.instructors.deleteOne({ _id: 3 });     
        console.log("Task 2 — Deleted Charlie (student) and Dr. Lee (instructor)");
    }

    // Task 3: Add Score array to 2 documents in each collection 
    async addScoreArray() {
        await this.students.updateMany(
            {_id: {$in: [1, 2]}},
            {$set: {Score: [10, 20, 30, 40]}}
        )

        await this.instructors.updateMany(
            { _id: { $in: [1, 2] } },
            { $set: { Score: [10, 20, 30, 40] } }
        );
        console.log("Task 3 — Score [10, 20, 30, 40] added to 2 students and 2 instructors");
    }

    // Task 4: Position-based Score insert 
    async applyScoreRules() {
        for (const col of [this.students, this.instructors] as Collection<any>[]) {
            await col.updateOne(
                { _id: 1 },
                { $push: { Score: { $each: [5], $position: 2 } } } as any
            );
            await col.updateMany(
                { _id: { $ne: 1 } },
                { $push: { Score: { $each: [6], $position: 3 } } } as any
            );
        }
        console.log("Task 4 — Score rules applied (5 @ index 2 for _id=1, 6 @ index 3 for _id=2)");
    }

    // Task 5: Multiply every Score element by 20 

    async multiplyScores(factor: number = 20) {
        await this.students.updateMany({}, { $mul: { "Score.$[]": factor } });
        await this.instructors.updateMany({}, { $mul: { "Score.$[]": factor } });
        console.log(`Task 5 — All Score elements multiplied by ${factor}`);
    }

    // Task 6: One-to-Many relationship already established via courses.instructorId
    async showRelationship() {
        await this.courses.deleteMany({})
        await this.courses.insertMany([
            { _id: 101, title: "Database Systems", instructorId: 1 },
            { _id: 102, title: "Web Development",  instructorId: 1 },
            { _id: 103, title: "Linear Algebra",   instructorId: 2 },
            { _id: 104, title: "MIS Fundamentals", instructorId: 3 },
        ]);
        const coursesByInstructor = await this.courses.find({}).toArray();
        console.log("\n📌 Part 2 — One-to-Many Relationship (courses.instructorId → instructors._id):");
        for (const course of coursesByInstructor) {
            const instructor = await this.instructors.findOne({ _id: course.instructorId });
            console.log(`   Course "${course.title}" (ID: ${course._id}) → Instructor: ${instructor?.name ?? "deleted"}`);
        }
    }
}

// Run 

async function main() {
    const service = new MongoService();
    try {
        await service.connect();
        await service.insertDocuments();   // Task 1
        await service.deleteOneEach();     // Task 2
        await service.addScoreArray();     // Task 3
        await service.applyScoreRules();   // Task 4
        await service.multiplyScores(20);  // Task 5
        await service.showRelationship();  // Task 6
        console.log("\nAll MongoDB tasks completed!");
    } finally {
        await service.disconnect();
    }
}

main();
// db.courses.aggregate([{'$lookup': {from: 'instructors', localField: 'instructorId', foreignField: '_id', as: 'instructor'}}, {'$project': {title: 1, instructor: 1}}])