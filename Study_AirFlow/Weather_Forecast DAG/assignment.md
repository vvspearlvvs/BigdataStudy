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
