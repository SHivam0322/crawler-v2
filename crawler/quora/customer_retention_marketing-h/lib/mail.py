import smtplib


def sendMail(msg):  
    try:
        sub = "test"
        server = smtplib.SMTP_SSL('smtp.gmail.com', 465)
        server.login("titash.canada@gmail.com", "Qwerty123#")
        server.sendmail(
            "titash.canada@gmail.com", 
            "skt1598@gmail.com",
            msg)
        server.quit()
        return True
    except Exception:
        return False