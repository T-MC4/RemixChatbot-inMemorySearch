import path from "path";
import fs from "fs/promises";
import crypto from "crypto";

// Recursively go through local directory
export const getFilesInDirectory = async (dir, fileList = []) => {
  const files = await fs.readdir(dir);
  for (const file of files) {
    const filePath = path.join(dir, file);
    const stat = await fs.stat(filePath);
    if (stat.isDirectory()) {
      fileList = getFilesInDirectory(filePath, fileList);
    } else {
      fileList.push(filePath);
    }
  }
  return fileList;
};

// Calculate the hash of the file
export const calculateFileHash = async (filePath) => {
  const data = await fs.readFile(filePath);
  return crypto.createHash("sha256").update(data).digest("hex");
};

// Function to delete a file from local
export const deleteFromLocal = (filePath) => {
  return fs.unlink(filePath);
};
