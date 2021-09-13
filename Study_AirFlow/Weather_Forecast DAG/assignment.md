### 과제 설명
Open Weathermap 무료 API를 사용해서 서울의 다음 7일간의 낮기온 평균(day)/최소(min)/최대(max)
온도를 읽어다가 각자 스키마 밑의 weather_forecast라는 테이블로 저장

### API 설명 
- 사용 API : openWeather (https://openweathermap.org/api/one-call-api)
- API KEY : 발급
- Request : https://api.openweathermap.org/data/2.5/onecall?lat={lat}&lon={lon}&appid={API key}
    - 서울의 위도(lat)/경도(lon) : 37.5665/126.9780
- Response : 
    daily":[
        {"dt":1631156400,"sunrise":1631135333,"sunset":1631181031,"moonrise":1631142720,"moonset":1631186340,"moon_phase":0.08,
         "temp":{"day":27.92,"min":18.47,"max":29.73,"night":23.17,"eve":26.22,"morn":19.14},
         "feels_like":{"day":28.27,"night":23.37,"eve":26.22,"morn":19.46},
         "pressure":1014,"humidity":49,"dew_point":15.96,"wind_speed":2.95,"wind_deg":275,"wind_gust":3.36,
         "weather":[{"id":803,"main":"Clouds","description":"broken clouds","icon":"04d"}],
         "clouds":65,"pop":0,"uvi":6.95}, .. {생략}


### WeatherETL_Airflow -ver1
- load 부분에서 임시 테이블을 사용하지 않고 원래 테이블을 다시 생성하고 원래 테이블에서 내용을 채우기 때문에 아마도 테이블의 내용이 매번 새로 업데이트되는 문제 -> 즉 과거 데이터들이 지워진다! 
- 요구사항은 incremental update 
  즉, 기존에 있던 과거날짜를 그대로 놔두고, 앞으로 일주일간 서울날씨를 업데이트 해야한다. 
- try-except에서 raise가 없어서 에러발생을 전파하지 않음

### WeatherETL_Airflow -ver2
- 과거날짜를 그대로 두고 최신 데이터만 업데이트하도록 변경 -> Temp 테이블 사용
    - 임시 테이블 생성 
    - 읽어온 데이터를 임시테이블에 insert
    - 임시테이블에서 최신날짜의 레코드를 찾고, 원본테이블에 insert (incremental update)
- try-except에서 raise를 추가해서 에러가 났다는걸 전파할 수 있도록 해줌(권장) 