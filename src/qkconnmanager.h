#ifndef QKCONNMANAGER_H
#define QKCONNMANAGER_H

#include <QObject>
#include <QHash>
#include <QHostAddress>
#include "qkconnworker.h"



class QkConnManager : public QObject
{
    Q_OBJECT
public:


    QkConnManager();

    bool add(int connType, QMap<QString,QString> args, int *id);
    bool remove(QString id);
    QkConnWorker* worker(int id);


signals:
    void message(int,QString);

private slots:
    void _slotMessageSentByWorker(int type, QString msg);

private:
    QHash<int,QkConnWorker*> _connWorkers;

    QHostAddress _host;
    static int _nextPort;
};

#endif // QKCONNMANAGER_H
