import webapp
import unittest
import json


def get_session(response):
    cookies = [header for header in response.headers
               if header[0] == "Set-Cookie"][0][1]
    session = cookies.split('=')[1]
    return session


class FlaskrTestCase(unittest.TestCase):
    def login(self, login, password):
        response = self.app.post('/login',
                                 data=json.dumps({
                                     "login": login,
                                     "password": password}))
        return response

    def setUp(self):
        with open('config.json') as f:
            self.config = json.loads(f.read())
        webapp.app.config['TESTING'] = True
        self.app = webapp.app.test_client()
        self.username = self.app.application.config["USER"]["LOGIN"]
        self.password = self.app.application.config["USER"]["PASSWORD"]

    def test_login(self):
        # incorrect login
        response = self.login("I hope", "it will fail")
        self.assertEqual(response.status_code, 401)

        # correct login
        response = self.login(self.username, self.password)
        self.assertEqual(response.status_code, 200)

    def test_search(self):
        response = self.login(self.username, self.password)
        session = get_session(response)
        self.app.set_cookie("session", session)
        response = self.app.post("/search", data=json.dumps({
            "UPN": "5853310887000180"
        }))
        self.assertEqual(response.status_code, 200)
        response = self.app.post("/search", data=json.dumps({
            "birthdate": "1981-03-10"
        }))
        self.assertEqual(response.status_code, 200)

    def test_check(self):
        response = self.login(self.username, self.password)
        session = get_session(response)
        self.app.set_cookie("session", session)
        response = self.app.post("/check", data=json.dumps({
            "UPN": "5853310887000180"
        }))
        self.assertEqual(response.status_code, 200)
        response = self.app.post("/check", data=json.dumps({
            "birthdate": "1981-03-10"
        }))
        self.assertEqual(response.status_code, 200)

if __name__ == '__main__':
    unittest.main()
