#include "qkconnworker.h"
#include "qkdaemon.h"
#include <QDebug>
#include <QProcess>
#include <QRegularExpression>
#include <QJsonObject>
#include <QJsonValue>

QkConnWorker::QkConnWorker(int port, QObject *parent) :
    QObject(parent)
{
    _host = QHostAddress(QHostAddress::LocalHost);
    _port = port;
    _process = new QProcess(this);
    _process->setProcessChannelMode(QProcess::MergedChannels);
    _process->setProgram(QkDaemon::QKCONNECT_EXE);

    _jsonParser = new QkUtils::JsonParser(this);

    connect(_jsonParser, SIGNAL(parsed(QJsonDocument)),
            this, SLOT(_slotJsonParsed(QJsonDocument)));

    connect(&_client, SIGNAL(readyRead()), this, SLOT(_slotClientReadyRead()));

    _connectProcessSignals(true);
}

QStringList QkConnWorker::_parseLines(QByteArray output, QString text)
{
    QStringList sl;
    QTextStream ts(output);
    while(1)
    {
        QString line = ts.readLine();
        if(line.isEmpty()) break;
        if(line.contains(text))
        {
            sl << line;
        }
    }
    return sl;
}

QString QkConnWorker::version()
{
    QProcess process;
    process.setProcessChannelMode(QProcess::MergedChannels);
    process.setProgram(QkDaemon::QKCONNECT_EXE);
    QString version = "";
    process.setArguments(QStringList() << "--version");
    process.start();
    if(process.waitForFinished(10000))
    {
        version = process.readAll();
    }
    else
    {
        qDebug() << "Failed to get QkConnect version";
    }
    version = version.remove('\n');
    return version;
}

QStringList QkConnWorker::listSerialPorts()
{
    QProcess process;
    process.setProcessChannelMode(QProcess::MergedChannels);
    process.setProgram(QkDaemon::QKCONNECT_EXE);
    QStringList portNames;
    process.setArguments(QStringList() << "--list-serial");
    process.start();
    if(process.waitForFinished(10000))
    {
        QStringList lines = _parseLines(process.readAll(), "[");
        foreach(QString line, lines)
        {
            portNames << line.split(' ', QString::SkipEmptyParts).at(1);
        }
    }
    else
    {
        qDebug() << "Failed to get QkConnect serial ports: " + process.errorString();
    }
    return portNames;
}

bool QkConnWorker::open(ConnType connType, QMap<QString, QString> params)
{
    QStringList processArgs;
    QHostAddress host(QHostAddress::LocalHost);
    processArgs << host.toString() << QString::number(_port);

    switch(connType)
    {
    case ctSerialPort:
        processArgs << "serial";
        processArgs << params["portname"];
        processArgs << params["baudrate"];
        processArgs << params["dtr"];
        break;
    default:
        return false;
    }

    processArgs << "--parse" << "--join";
    processArgs << "--no-cli";
    processArgs << "--verbose";

    qDebug() << __PRETTY_FUNCTION__ << processArgs;
    _process->setProgram(QkDaemon::QKCONNECT_EXE);
    _process->setArguments(processArgs);
    _process->start();


    return true;
}

void QkConnWorker::close()
{
    if(_process->isOpen())
    {
        _process->close();
    }
}

int QkConnWorker::port()
{
    return _port;
}

void QkConnWorker::_slotClientReadyRead()
{
    _jsonParser->parseData(_client.readAll());
}

void QkConnWorker::_slotProcessReadyRead()
{
    int retries = 10;

    while(_process->bytesAvailable() > 0 && retries--)
    {
        QByteArray output = _process->readAll();
        QTextStream ts(output);
        while(1)
        {
            QString line = ts.readLine();
            if(line.isEmpty() && ts.atEnd()) break;
//            qDebug() << "stdout:" << line;

            if(line.contains("Ready") && _ws != wsReady)
            {
                _ws = wsReady;
                _client.connectToHost(_host, _port);
            }
        }
    }
}

void QkConnWorker::_slotJsonParsed(QJsonDocument doc)
{
    qDebug() << __PRETTY_FUNCTION__;
    QJsonObject obj = doc.object();
    qDebug() << doc.toJson();
    if(obj.value("type").toString() == "status")
    {
        QString msg = obj.value("message").toString();
        QStringList fields = msg.split(' ', QString::SkipEmptyParts);
        QString timestamp = fields.takeFirst();
        QString msgType = fields.takeFirst();
        QString msgToken = fields.takeFirst();
        QString msgBody = fields.join(" ");

        (void) timestamp;
        QkDaemon::MessageType msgTypeEnum;

        if(msgType == "[i]")
        {
            msgTypeEnum = QkDaemon::MESSAGE_INFO;
        }
        else if(msgType == "[e]")
        {
            msgTypeEnum = QkDaemon::MESSAGE_ERROR;
        }
        else if(msgType == "[d]")
        {
            msgTypeEnum = QkDaemon::MESSAGE_DEBUG;
        }

        emit message((int)msgTypeEnum, msgToken + " " + msgBody);
    }
}

void QkConnWorker::_slotProcessFinished(int status)
{
    qDebug() << __PRETTY_FUNCTION__ << status;
}

void QkConnWorker::_slotProcessError(QProcess::ProcessError error)
{
    qDebug() << __PRETTY_FUNCTION__ << _process->errorString();
    emit message(QkDaemon::MESSAGE_ERROR,
                 _process->errorString());
}

void QkConnWorker::_processWait()
{
    if(_process->isOpen())
        _process->waitForFinished();
}

void QkConnWorker::_connectProcessSignals(bool en)
{
    if(en)
    {
        connect(_process, SIGNAL(readyRead()),
                this, SLOT(_slotProcessReadyRead()));
        connect(_process, SIGNAL(finished(int)),
                this, SLOT(_slotProcessFinished(int)));
        connect(_process, SIGNAL(error(QProcess::ProcessError)),
                this, SLOT(_slotProcessError(QProcess::ProcessError)));
    }
    else
    {
        disconnect(_process, SIGNAL(finished(int)),
                this, SLOT(_slotProcessFinished(int)));
        disconnect(_process, SIGNAL(error(QProcess::ProcessError)),
                this, SLOT(_slotProcessError(QProcess::ProcessError)));
    }
}

