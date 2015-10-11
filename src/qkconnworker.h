#ifndef QKCONNECTWORKER_H
#define QKCONNECTWORKER_H

#include "qkutils.h"

#include <QObject>
#include <QStringList>
#include <QProcess>
#include <QHostAddress>
#include <QTcpSocket>

class QkConnWorker : public QObject
{
    Q_OBJECT
public:
    enum ConnType
    {
        ctSerialPort
    };
    enum WorkerState
    {
        wsCreated = 0,
        wsReady,
        wsClosed
    };

    explicit QkConnWorker(int port, QObject *parent = 0);

    void setID(int id) { _id = id; }
    int id() { return _id; }

    bool open(ConnType connType, QMap<QString,QString> params);
    void close();

    int port();

    static QString version();
    static QStringList listSerialPorts();

signals:
    void message(int,QString);

private slots:
    void _slotProcessReadyRead();
    void _slotProcessFinished(int status);
    void _slotProcessError(QProcess::ProcessError error);
    void _slotClientReadyRead();
    void _slotJsonParsed(QJsonDocument doc);


private:
    void _connectProcessSignals(bool en);

    int _id;
    QProcess *_process;
    QHostAddress _host;
    int _port;
    WorkerState _ws;
    QTcpSocket _client;
    QkUtils::JsonParser *_jsonParser;

    void _processWait();
    static QStringList _parseLines(QByteArray output, QString text);

};

#endif // QKCONNECTWORKER_H
