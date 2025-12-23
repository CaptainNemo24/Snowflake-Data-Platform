from openai import OpenAI
import json
import time

# OpenAI API key
client = OpenAI(
api_key="my api key"
)

# OpenAI API 이용
def translate(text):
    prompt = f"""
    지금부터 제공할 JSON 객체에서 아래 세 가지 항목의 **값(value)**만 자연스럽고 정확하게 번역해줘:
    1. `"tag"`: 이 항목은 **대화의 주제 또는 카테고리**야. 예: greeting → 인사
    2. `"patterns"`: 이 항목은 사용자의 질문 예시들이고, **구어체 자연스러운 질문 형태로 번역**해.
    3. `"responses"`: 이 항목은 챗봇이 사용자에게 응답하는 문장들이고, **정중하고 자연스럽게 번역**해.

    다음 사항을 반드시 지켜:
    - **JSON 키는 절대 수정하지 마.**
    - 각 값은 **의미가 잘 통하도록 자연스럽게 번역해.**
    - **구조(JSON 형식)는 그대로 유지**해.
    - 절대 JSON 전체를 문자열로 감싸거나 백슬래시(\\), 줄바꿈 기호(\\n)를 넣지 마.
    - 출력은 유효한 JSON으로 해줘.

    다음은 번역할 JSON 객체야:
    {json.dumps(item, ensure_ascii=False, indent=2)}
    """
    
    try:
        completion = client.chat.completions.create(
            model="gpt-3.5-turbo-1106",
            messages=[
                {"role": "system", "content": "너는 전문 영어-한국어 번역가야."},
                {"role": "user", "content": prompt}
            ]
        )
        translated = completion.choices[0].message.content.strip()
        try:
            translated_json = json.loads(translated)
            # print(f"번역 성공: {item['tag']} → {translated_json['tag']}")
            return translated_json
        except json.JSONDecodeError:
            print("JSON 파싱 실패 — 원문 반환")
            return item
    except Exception as e:
        print(f" 오류 발생: {e}")
        # 오류 발생 시 원문 그대로 반환
        return text

# JSON 불러오기
with open(fr'filepath\snowflake-medallion-platform\python-utils\chatbot\sample_data\intents.json', 'r', encoding='utf-8') as f:
    data = json.load(f)


# 각 intent 번역
translated_intents = []
for item in data["intents"]:
    translated_item = translate(item)
    translated_intents.append(translated_item)
    time.sleep(1)

# 새 JSON 저장
result = {"intents": translated_intents}

# 번역 완료된 파일 저장
with open(fr'filepath\snowflake-medallion-platform\python-utils\chatbot\result\intents.json', 'w', encoding='utf-8') as f:
    json.dump(result, f, ensure_ascii=False, indent=2)

print("번역 완료!")
