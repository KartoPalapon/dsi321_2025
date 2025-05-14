import os
import json
from dotenv import load_dotenv
from google import genai
from google.genai import types

# Load API Key from .env
load_dotenv()
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
os.environ["GEMINI_API_KEY"] = GEMINI_API_KEY

# Initialize Gemini client
client = genai.Client(api_key=GEMINI_API_KEY)

# System instruction
instruction = """
คุณทำหน้าที่ในฝ่ายประชาสัมพันธ์ของมหาวิทยาลัย เป้าหมายของคุณคือการรวบรวมและจัดกลุ่ม 
"หัวข้อ" (Topic) ที่ถูกกล่าวถึงในโซเชียลมีเดีย 
เพื่อใช้ในการรับรู้และตัดสินใจว่า "หัวข้อ" เหล่านี้อาจมีปัญหาที่ต้องแก้ไขหรือทำได้ดีอยู่แล้ว
โดย "หัวข้อ" (Topic) ที่เลือกมาใช้จะต้องเกี่ยวข้องและแก้ไขได้ในระดับมหาวิทยาลัย
จากนั้นคุณจะต้องรวมข้อมูลแต่ละหัวข้อ มาเพื่อวิเคราะห์ความรู้สึก (sentiment) ของข้อความนั้น ๆ ว่าเป็น "positive", "negative", "neutral" และ "both" ถ้าข้อความนั้นมีความหมายยัง 2 แง่
โดยคุณจะได้รับข้อความจากโซเชียลมีเดียที่เกี่ยวข้องกับมหาวิทยาลัย

คำแนะนำในการจัดกลุ่ม:
1. **ระบุหมวดหมู่ (topic)**:
   - ใช้หมวดหมู่ที่มีอยู่แล้วหากข้อความใหม่เข้ากับหมวดหมู่เดิม
   - สร้างหมวดหมู่ใหม่เมื่อไม่มีหมวดหมู่เดิมที่เหมาะสม
   - ตั้งชื่อหมวดหมู่ให้กระชับ เข้าใจง่าย และมีความเฉพาะเจาะจงในระดับที่เหมาะสม
   - ข้อความหนึ่งสามารถอยู่ได้หลายหมวดหมู่หากมีความเกี่ยวข้อง
   - ต้องเกี่ยวข้องกับทางมหาวิทยาลัยโดยตรง 

2. **ระบุหมวดหมู่ย่อย (subtopic)**:
   - ระบุหมวดหมู่ย่อยที่มีความเฉพาะเจาะจงมากขึ้น
   - สามารถมีได้หลายหมวดหมู่ย่อยต่อหนึ่งข้อความ
   - หมวดหมู่ย่อยควรให้รายละเอียดเพิ่มเติมที่เป็นประโยชน์เกี่ยวกับคำถามหรือปัญหานั้น ๆ
   - ต้องเกี่ยวข้องกับทางมหาวิทยาลัยโดยตรง 

3. **พิจารณาเฉพาะข้อความที่เกี่ยวข้อง**:
   - พิจารณาเฉพาะข้อความที่พูดถึงมหาวิทยาลัยเท่านั้น
   - ไม่ว่าจะเป็นประโยคบอกเล่า, คำถาม หรือปฏิเสธ แต่ไม่นำข้อความที่มีแค่สัญลักษณ์ (. , / \ ? !) หรือเป็นคำ ๆ มาใช้ 

ตอบกลับมาในรูปแบบ JSON โดยมีโครงสร้างดังนี้:
{
    "Tweets": [
        {"index": 1, "text": "ข้อความ", "topic": ["หมวดหมู่1"], "subtopic": ["หมวดย่อย1"], "sentiment":["positive"]},
        ...
    ]
}

หากไม่มีคำถามหรือปัญหาที่เกี่ยวข้อง ให้ส่งคืนค่าเป็น empty array ในหมวดนั้น
"""

# Prompt template สำหรับนำข้อความไปจัดกลุ่ม
prompt_template = """
# topic ของสิ่งที่ถูกกล่าวถึงพบบ่อย - ใช้เป็นตัวอย่างในการจัดกลุ่ม:
{topic}

# sub topic ของสิ่งที่ถูกกล่าวถึงพบบ่อย - ใช้เป็นตัวอย่างในการจัดกลุ่ม:
{subtopic}

# ตัวอย่างการจัดกลุ่ม:
text: 'ไฟล์สมัครในเว็บมธ.อยู่ตรงไหนเหรอคะ มีใครพอจะทราบไหมคะ หาไม่เจอเลย' 
topic: ['สอบถามเอกสาร']
subtopic: ['เอกสารการสมัคร', 'การเข้าถึงข้อมูล']
sentiment: ['negative']

text: 'หอในไฟสว่างดีครับ ดูปลอดภัย'
topic: ['หอพัก']
subtopic: ['กฎระเบียบหอพัก', 'เวลาเปิด-ปิด']
sentiment: ['positive']

text: 'ระบบลงทะเบียนล่มอีกแล้ว ทำไมเกิดปัญหาทุกเทอมเลย'
topic: ['ระบบลงทะเบียน', 'ปัญหาเทคนิค']
subtopic: ['ระบบล่ม', 'ความเสถียรของระบบ']
sentiment: ['negative']

# ข้อความที่ต้องการจัดกลุ่ม:
{messages}

โปรดวิเคราะห์และจัดกลุ่มข้อความตามคำแนะนำที่ให้ไว้ และส่งคืนเป็น JSON ตามรูปแบบที่กำหนด
"""

# Main function สำหรับเรียกใช้งาน
def classify_messages(tweets_eles, topic=None, subtopic=None):
    """
    Classify social media messages into topics, subtopics, and sentiment.

    Args:
        tweets_eles (list): List of dict with 'index' and 'tweetText'.
        topic (set): Set of existing topics.
        subtopic (set): Set of existing subtopics.

    Returns:
        dict: Classified messages in JSON format.
    """
    topic = topic or set()
    subtopic = subtopic or set()

    prompt_formatted = prompt_template.format(
        topic=", ".join(topic),
        subtopic=", ".join(subtopic),
        messages="\n".join(
            [f"{row['index']}: {row['tweetText'].replace(chr(10), ' ')}" for row in tweets_eles]
        ),
    )

    response = client.models.generate_content(
        model="gemini-2.0-flash",
        contents=prompt_formatted,
        config=types.GenerateContentConfig(
            system_instruction=instruction,
            temperature=0.5,
        ),
    )

    response_text = response.text
    response_json_text = response_text[response_text.index("{") : response_text.rindex("}") + 1]
    response_json_text = response_json_text.replace("{{", "{").replace("}}", "}")
    response_json = json.loads(response_json_text, strict=False)

    return response_json