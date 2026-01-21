import os
import glob
import pandas as pd
from ultralytics import YOLO

# Paths
BASE_PATH = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
IMAGE_DIR = os.path.join(BASE_PATH, "data", "raw", "images")
OUTPUT_CSV = os.path.join(BASE_PATH, "data", "yolo_results.csv")

# Load YOLOv8 nano model
model = YOLO('yolov8n.pt')

def classify_image(detections):
    """
    Categorize image based on detected objects:
    - promotional: Contains person + product
    - product_display: Contains bottle/container, no person
    - lifestyle: Contains person, no product
    - other: Neither detected
    """
    has_person = any(d['name'] == 'person' for d in detections)
    # Define 'product' as bottle, cup, vase, or bowl (common containers)
    product_classes = ['bottle', 'cup', 'vase', 'bowl']
    has_product = any(d['name'] in product_classes for d in detections)
    
    if has_person and has_product:
        return 'promotional'
    elif has_product and not has_person:
        return 'product_display'
    elif has_person and not has_product:
        return 'lifestyle'
    else:
        return 'other'

def run_detection():
    # Find all images
    image_paths = glob.glob(os.path.join(IMAGE_DIR, "**", "*.jpg"), recursive=True)
    results_data = []
    
    print(f"Found {len(image_paths)} images to process.")
    
    for img_path in image_paths:
        # Extract metadata from path
        # Expected path: .../data/raw/images/{channel_name}/{message_id}.jpg
        parts = os.path.normpath(img_path).split(os.sep)
        channel_name = parts[-2]
        message_id = os.path.splitext(parts[-1])[0]
        
        # Run YOLO detection (remove unsupported 'quiet' arg)
        try:
            results = model(img_path)
        except Exception as e:
            print(f"Skipping image {img_path} due to read/prediction error: {e}")
            continue
        
        detections = []
        for result in results:
            for box in result.boxes:
                cls_id = int(box.cls[0])
                name = model.names[cls_id]
                conf = float(box.conf[0])
                detections.append({'name': name, 'conf': conf})
        
        # Determine category
        category = classify_image(detections)
        
        # Get primary detection (highest confidence) for the 'detected_class' field
        if detections:
            primary = max(detections, key=lambda x: x['conf'])
            det_class = primary['name']
            conf_score = primary['conf']
        else:
            det_class = 'none'
            conf_score = 0.0
            
        results_data.append({
            'message_id': message_id,
            'channel_name': channel_name,
            'detected_class': det_class,
            'confidence_score': conf_score,
            'image_category': category
        })
        
    # Save to CSV
    df = pd.DataFrame(results_data)
    df.to_csv(OUTPUT_CSV, index=False)
    print(f"Detection completed. Results saved to {OUTPUT_CSV}")

if __name__ == "__main__":
    run_detection()
