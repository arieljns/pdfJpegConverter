const express = require("express");
const bodyParser = require("body-parser");
const cors = require("cors");
const multer = require("multer");
const pdfPoppler = require("pdf-poppler");
const fs = require("fs");
const path = require("path");
const archiver = require("archiver");

const app = express();

app.use(cors());
app.use(express.json());
app.use(bodyParser.json());
app.use(bodyParser.urlencoded({ extended: false }));
const storage = multer.memoryStorage();

const upload = multer({ storage: storage });



let completedTask = 0
let totalTask = 0



app.post("/upload", upload.array("file"), async (req, res) => {

    const files = req.files;
    console.log("This is the file from frontend:", files)
    const { totalChunks, currentChunk, filename } = req.body;

    req.body.filename = filename.replace("WS.pdf", "ST2023")
    console.log("this is the file filename:", req.body.filename)

    try {
        const outputDir = `${process.env.DIRECTORY}+/petaWS-Chunk20`;
        const zipFilePath = path.join(outputDir, 'converted-images.zip');
        const archive = archiver('zip');
        const output = fs.createWriteStream(zipFilePath);

        archive.pipe(output);

        const conversionPromises = files.map(async (file, i) => {
            const tempFilePath = path.join(outputDir, `temp-${req.body.filename}`);
            await fs.promises.appendFile(tempFilePath, file.buffer);

            if (currentChunk == totalChunks - 1) {
                const options = {
                    format: 'jpeg',
                    out_dir: outputDir,
                    out_prefix: `${req.body.filename}`,
                    scale: 4096,
                };

                console.log(`Converting file: ${tempFilePath}`);
                await pdfPoppler.convert(tempFilePath, options);
                console.log(`Conversion successful for file: ${tempFilePath}`);

                const oldFilePath = path.join(outputDir, `${req.body.filename}`);
                const newFilePath = path.join(outputDir, `${req.body.filename}.jpg`);

                if (fs.existsSync(oldFilePath)) {
                    await fs.promises.rename(oldFilePath, newFilePath);
                } else {
                    console.error("Source file not found:", oldFilePath);
                }

                archive.file(newFilePath, { name: `converted-${req.body.filename}-${i}.jpg` });

                completedTask++;

                if (completedTask === totalTask) {
                    archive.finalize();
                    console.log("The files have been successfully converted and inserted into the zip");

                    completedTask = 0;
                    totalTask = 0;
                }
            }
        });

        await Promise.all(conversionPromises);

        return res.json({
            message: "The file chunks have been received and assembled. Conversion tasks have been enqueued.",
            zipFileUrl: `/download/converted-images.zip`,
        });
    } catch (error) {
        console.error("Error during file processing:", error);
        res.status(500).json({
            message: "Internal Server Error",
            ErrorMessage: error,
        });
    }
});


app.get("/download/converted-images.zip", (req, res) => {
    const zipFilePath = "C:/Users/ariel/Downloads/converted-images.zip";

    console.log(`Checking file existence at path: ${zipFilePath}`);
    if (fs.existsSync(zipFilePath)) {
        console.log("File exists. Proceeding with download.");
        res.download(zipFilePath, "converted-images.zip", (err) => {
            if (err) {
                console.error("Error downloading file:", err);
                res.status(500).json({
                    message: "Internal Server Error",
                    ErrorMessage: err,
                });
            }
        });
    } else {
        console.error("File not found at the specified path.");
        res.status(404).json({
            message: "File not found",
        });
    }
});



app.listen(8080, async () => {
    console.log("Server is up and running at PORT 8080");

});
