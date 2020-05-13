from flask import Flask, render_template, request, url_for, redirect
import datetime

from KafkaProducerWithoutAvro import ProducerClassWithoutAvro

app = Flask(__name__)


@app.route('/')
def index():
    return render_template("Hello.html")


@app.route('/complete')
def complete():
    return "ProcessingCompolete"


@app.route('/processed', methods=['POST', 'GET'])
def processed():
    if request.method == 'POST':
        payment_info = request.form.to_dict()
        payment_info['DT'] = str(datetime.datetime.now())
        t = ProducerClassWithoutAvro()
        t.main(payment_info)
        return redirect(url_for('complete'))


if __name__ == '__main__':
    app.run(debug=True)
    pass



