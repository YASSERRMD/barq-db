# Image Search Demo (TypeScript)

This example demonstrates how to building an Image Search application using Barq DB and TypeScript.

## Prerequisites

- Node.js 18+
- Barq DB running locally

## Setup

1. Install dependencies:
   ```bash
   npm install
   ```

2. (Optional) Run Python CLIP service (not included in this simple demo, we use mock vectors).

## Usage

```bash
npm start
```

## What it does

1. Connects to Barq DB.
2. Creates an `image_gallery` collection.
3. Inserts metadata for sample images (simulating CLIP embeddings).
4. Performs a search for "similar images" (vector search).
