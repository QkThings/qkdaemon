#ifndef QKDAEMON_H
#define QKDAEMON_H

#include <QObject>
#include "qkconnworker.h"
#include "qkconnmanager.h"

class QkDaemon : public QObject
{
    Q_OBJECT
public:
    static const QString QKCONNECT_EXE;

    enum MessageType
    {
        MESSAGE_INFO,
        MESSAGE_ERROR,
        MESSAGE_DEBUG
    };

    QkDaemon(QObject *parent = 0);

    QkConnManager connManager;

};

#endif // QKDAEMON_H
