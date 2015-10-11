/*
 * QkThings LICENSE
 * The open source framework and modular platform for smart devices.
 * Copyright (C) 2014 <http://qkthings.com>
 * 
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#include "qkdaemonthread.h"

#include "qkdaemonsocket.h"
//#include "qkconnect.h"
#include "qkapihandler.h"

#include <QJsonDocument>

QkDaemonThread::QkDaemonThread(int socketDescriptor, QObject *parent) :
    QThread(parent),
    m_socketDescriptor(socketDescriptor)
{
    m_socket = 0;
    //m_connManager = connManager;
    m_apiHandler = new QkAPIHandler(connManager, this);
    //m_apiHandler->moveToThread(this);
    _setupConnections();
}

void QkDaemonThread::_setupConnections()
{
    connect(m_apiHandler, SIGNAL(sendJson(QJsonDocument)), this, SLOT(sendJson(QJsonDocument)), Qt::DirectConnection);
}

QkDaemonSocket* QkDaemonThread::socket()
{
    return m_socket;
}

void QkDaemonThread::run()
{
    QkDaemonSocket socket;
    m_socket = &socket;

    if(!m_socket->setSocketDescriptor(m_socketDescriptor))
    {
        emit error(m_socket->error());
        return;
    }
    emit clientConnected(m_socketDescriptor);
    connect(m_socket, SIGNAL(parsedJsonString(QString)), this, SLOT(_handleParsedJsonString(QString)), Qt::DirectConnection);
    connect(m_socket, SIGNAL(disconnected()), this, SLOT(_handleDisconnected()), Qt::DirectConnection);
    exec();
}

void QkDaemonThread::sendJson(const QJsonDocument &doc)
{
    qDebug() << "QkDaemonThread::sendJson()";
    QByteArray data;
    if(!doc.isEmpty())
        data = doc.toJson(QJsonDocument::Compact);

    if(m_socket->isOpen() && !data.isEmpty())
        m_socket->write(data);
}

void QkDaemonThread::_handleParsedJsonString(QString str)
{
    qDebug() << "QkDaemonThread::_handleParsedJsonString()" << str;
    m_apiHandler->handleJsonRpc(str);
}

void QkDaemonThread::_handleDisconnected()
{
    disconnect(m_apiHandler, SIGNAL(sendJson(QJsonDocument)), this, SLOT(sendJson(QJsonDocument)));
    m_apiHandler->setRealTime(false);
    emit clientDisconnected(m_socketDescriptor);
    quit();
}

